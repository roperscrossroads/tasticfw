#include "ARRModule.h"
#include "MeshService.h"
#include "NodeDB.h" 
#include "RTC.h"
#include "main.h"
#include "modules/NodeInfoModule.h"

#include "mesh/Router.h"
#include <cstring>
#include <map>

extern Router *router;

ARRModule *arrModule;

ARRModule::ARRModule() 
    : SinglePortModule("ARR", meshtastic_PortNum_PRIVATE_APP),
      concurrency::OSThread("ARR")
{
    // Only enable for CLIENT and CLIENT_MUTE roles
    if (config.device.role == meshtastic_Config_DeviceConfig_Role_CLIENT ||
        config.device.role == meshtastic_Config_DeviceConfig_Role_CLIENT_MUTE) {
        
        moduleEnabled = true;
        
        // TODO: Load priority shortnames from configuration instead of hardcoding
        // For now, keep the test configuration but add validation
        const char* defaultPriorityNodes[] = {
            "rxr1", "CSR1", "7e4c", "WOLF", "TCMQ", "CSR5", "CS11"
        };
        
        LOG_WARN("ARR: Using hardcoded priority nodes (should be configurable)");
        for (size_t i = 0; i < sizeof(defaultPriorityNodes) / sizeof(defaultPriorityNodes[0]); i++) {
            if (!addPriorityShortname(String(defaultPriorityNodes[i]))) {
                LOG_WARN("ARR: Failed to add priority node: %s", defaultPriorityNodes[i]);
            }
        }
        
        // Start with a short delay to let the mesh settle
        setIntervalFromNow(30 * 1000); // 30 seconds initial delay
    } else {
        moduleEnabled = false;
        disable();
    }
}

int32_t ARRModule::runOnce()
{
    if (!moduleEnabled) {
        LOG_WARN("ARR: runOnce called on disabled module");
        return EVALUATION_INTERVAL; // Shouldn't happen, but safety check
    }

    uint32_t now = millis();
    
    // Only do full evaluation every EVALUATION_INTERVAL
    if (now - lastEvaluation < EVALUATION_INTERVAL) {
        return FAST_CHECK_INTERVAL; // Check again sooner rather than using magic number
    }
    
    evaluationCount++;
    lastEvaluation = now;
    
    LOG_DEBUG("ARR: Starting evaluation #%d", evaluationCount);
    
    // Always respect minimum role change interval
    if (shouldDelayRoleChange()) {
        LOG_DEBUG("ARR: Role change delayed due to cooldown");
        return getNextCheckInterval();
    }

    // Check for strong signal override - this is our only decision criteria
    bool strongSignalOverride = checkStrongSignalOverride();
    
    // Request fresh info for stale priority nodes (this happens in parallel)
    requestNodeInfoFromStalePriorityNodes();
    
    // Simple decision: mute if any priority node has strong signal, otherwise stay CLIENT
    bool shouldMute = strongSignalOverride;
    bool currentlyMuted = getCurrentMuteState();

    if (shouldMute != currentlyMuted) {
        // Role change needed
        const char* reason = strongSignalOverride ? "strong priority node signal" : "weak priority node signals";
        
        applyMuteDecision(shouldMute, reason);
        lastRoleChange = now;
        networkIsStable = false;
        
        LOG_INFO("ARR: Role change %s->%s: %s",
                 currentlyMuted ? "CLIENT_MUTE" : "CLIENT",
                 shouldMute ? "CLIENT_MUTE" : "CLIENT", 
                 reason);
        
        // Check sooner after a change to monitor stability
        return FAST_CHECK_INTERVAL;
    } else {
        // No change needed - network appears stable
        networkIsStable = true;
        
        return getNextCheckInterval();
    }
}

bool ARRModule::checkStrongSignalOverride()
{
    if (priorityShortnames.empty()) {
        return false; 
    }
    
    if (!nodeDB || !nodeDB->meshNodes) {
        LOG_WARN("ARR: NodeDB not available");
        return false;
    }
    
    bool hasStrongSignal = false;
    RTCQuality timeQuality = getRTCQuality();
    
    // Log time quality for debugging
    LOG_DEBUG("ARR: Evaluating with time quality: %s", RtcName(timeQuality));

    for (int i = 0; i < nodeDB->numMeshNodes; i++) {
        auto node = nodeDB->getMeshNodeByIndex(i);
        if (!node || !node->has_user) continue;

        // Check if this is a priority node
        bool isPriorityNode = false;
        for (const auto& priority : priorityShortnames) {
            if (strcasecmp(node->user.short_name, priority.c_str()) == 0) {
                isPriorityNode = true;
                break;
            }
        }

        if (!isPriorityNode) continue;

        // DIRECT LINKS ONLY: Skip MQTT and multi-hop messages
        if (node->via_mqtt) {
            LOG_DEBUG("ARR: Skipping MQTT node %s", node->user.short_name);
            continue;
        }
        
        if (node->has_hops_away && node->hops_away > 0) {
            LOG_DEBUG("ARR: Skipping multi-hop node %s (hops: %d)", node->user.short_name, node->hops_away);
            continue;
        }

        // Use improved node data reliability assessment
        if (!isNodeDataReliable(node)) {
            LOG_DEBUG("ARR: Node %s data not reliable (age: %ds, quality: %s)", 
                     node->user.short_name, getNodeAge(node), RtcName(timeQuality));
            continue;
        }

        // Check SNR threshold - now we can trust it since it's a direct link with reliable data
        if (node->snr >= STRONG_SIGNAL_OVERRIDE_SNR) {
            hasStrongSignal = true;
            // Use current valid time if available, otherwise use millis() for tracking
            if (timeQuality >= RTCQualityFromNet) {
                lastStrongSignalOverride = getValidTime(RTCQualityFromNet);
            } else {
                lastStrongSignalOverride = millis() / 1000; // Convert to seconds for consistency
            }
            LOG_INFO("ARR: Strong signal detected from priority node %s (SNR: %.1fdB)", 
                    node->user.short_name, node->snr);
            break; // One strong signal is enough
        } else {
            LOG_DEBUG("ARR: Priority node %s signal too weak (SNR: %.1fdB < %.1fdB)", 
                     node->user.short_name, node->snr, STRONG_SIGNAL_OVERRIDE_SNR);
        }
    }

    strongSignalOverrideActive = hasStrongSignal;
    return hasStrongSignal;
}

void ARRModule::refreshRouterCache()
{
    cachedRouters.clear();
    totalRouterCount = 0;
    
    if (!nodeDB || !nodeDB->meshNodes) {
        LOG_WARN("ARR: NodeDB not available for router cache refresh");
        return;
    }
    
    uint32_t now = getTime(); // Use consistent time source with sinceLastSeen()

    for (int i = 0; i < nodeDB->numMeshNodes; i++) {
        auto node = nodeDB->getMeshNodeByIndex(i);
        
        if (!node) continue;
        
        if (node->via_mqtt) {
            continue;
        }
        
        if (node->has_hops_away && node->hops_away > 0) {
            continue;
        }
        
        if (!isValidRouter(node)) {
            continue;
        }

        RouterInfo ri = {};
        ri.nodeId = node->num;
        ri.snr = node->snr;
        
        cachedRouters.push_back(ri);
        totalRouterCount++;  // Simple count
    }

    lastNodeDBUpdate = nodeDB->meshNodes->size();
    LOG_DEBUG("ARR: Router cache refreshed: %d routers found", totalRouterCount);
}

bool ARRModule::isValidRouter(const meshtastic_NodeInfoLite *node) const
{
    if (!node) return false;

    // Must be a routing-capable role
    if (node->user.role != meshtastic_Config_DeviceConfig_Role_ROUTER &&
        node->user.role != meshtastic_Config_DeviceConfig_Role_ROUTER_LATE &&
        node->user.role != meshtastic_Config_DeviceConfig_Role_REPEATER) {
        return false;
    }

    // Must be recently active (15 minutes) - use NodeDB's method
    if (getNodeAge(node) > ROUTER_TIMEOUT_SEC) {
        return false;
    }

    // DIRECT LINKS ONLY: Must be radio-reachable (not MQTT-only)
    if (node->via_mqtt) {
        return false;
    }

    // DIRECT LINKS ONLY: Must be directly reachable (no multi-hop)
    if (node->has_hops_away && node->hops_away > 0) {
        return false;
    }

    // Must be on our primary channel (simplified check)
    if (node->channel != 0 && node->channel != channels.getPrimaryIndex()) {
        return false;
    }

    return true;
}

bool ARRModule::getCurrentMuteState() const
{
    return (config.device.role == meshtastic_Config_DeviceConfig_Role_CLIENT_MUTE);
}

void ARRModule::applyMuteDecision(bool shouldMute, const char* reason)
{
    auto currentRole = config.device.role;
    auto targetRole = shouldMute ? 
        meshtastic_Config_DeviceConfig_Role_CLIENT_MUTE : 
        meshtastic_Config_DeviceConfig_Role_CLIENT;

    if (currentRole != targetRole) {
        bool wasMuted = (currentRole == meshtastic_Config_DeviceConfig_Role_CLIENT_MUTE);
        config.device.role = targetRole;
        nodeDB->saveToDisk();
        LOG_INFO("ARR: Role changed to %s",
                 shouldMute ? "CLIENT_MUTE" : "CLIENT");
        // Broadcast status message to ARR channel  
        broadcastRoleChange(wasMuted, shouldMute, reason);
    }
}

bool ARRModule::shouldDelayRoleChange() const
{
    uint32_t now = millis();
    
    if (lastRoleChange == 0) return false; // First time
    
    uint32_t timeSinceLastChange = now - lastRoleChange;
    if (timeSinceLastChange < MIN_CHANGE_INTERVAL) {
        return true; // Still in cooldown period
    }
    
    return false; // Cooldown expired
}

uint32_t ARRModule::getNextCheckInterval()
{
    // Use faster checks when network seems unstable
    return networkIsStable ? EVALUATION_INTERVAL : FAST_CHECK_INTERVAL;
}

uint32_t ARRModule::getNodeAge(const meshtastic_NodeInfoLite *node) const
{
    if (!node) {
        return UINT32_MAX; // Invalid node is considered infinitely old
    }
    
    // Use NodeDB's built-in method which handles time quality and clock drift properly
    return sinceLastSeen(node);
}

bool ARRModule::isNodeDataReliable(const meshtastic_NodeInfoLite *node) const
{
    if (!node) {
        return false;
    }
    
    uint32_t nodeAge = getNodeAge(node);
    RTCQuality timeQuality = getRTCQuality();
    
    // With high-quality time (GPS/NTP), use strict age limits
    if (timeQuality >= RTCQualityNTP) {
        return nodeAge <= STRONG_SIGNAL_TIMEOUT_SEC;
    }
    
    // With medium-quality time (network/device), be more lenient
    if (timeQuality >= RTCQualityFromNet) {
        return nodeAge <= (STRONG_SIGNAL_TIMEOUT_SEC * 2); // Double the timeout
    }
    
    // With poor time quality, use alternative indicators
    // - Recent SNR data suggests active communication
    // - Low hop count suggests direct connectivity
    if (timeQuality == RTCQualityDevice || timeQuality == RTCQualityNone) {
        // Be very lenient with timing, focus on signal quality
        return (nodeAge <= (STRONG_SIGNAL_TIMEOUT_SEC * 4)) && 
               (node->snr > -20.0f) && // Reasonable signal strength
               (!node->has_hops_away || node->hops_away <= 1); // Direct or 1-hop
    }
    
    return false;
}

bool ARRModule::addPriorityShortname(const String& shortname)
{
    // Input validation
    if (shortname.isEmpty() || shortname.length() > MAX_SHORTNAME_LENGTH) {
        LOG_WARN("ARR: Invalid shortname length: %d (max: %d)", shortname.length(), MAX_SHORTNAME_LENGTH);
        return false;
    }
    
    if (priorityShortnames.size() >= MAX_PRIORITY_SHORTNAMES) {
        LOG_WARN("ARR: Maximum priority nodes limit reached: %d", MAX_PRIORITY_SHORTNAMES);
        return false;
    }
    
    // Check if already exists (case-insensitive)
    for (const auto& existing : priorityShortnames) {
        if (existing.equalsIgnoreCase(shortname)) {
            LOG_DEBUG("ARR: Priority node already exists: %s", shortname.c_str());
            return false;
        }
    }
    
    priorityShortnames.push_back(shortname);
    LOG_INFO("ARR: Added priority node: %s (%d total)", shortname.c_str(), priorityShortnames.size());
    return true;
}

bool ARRModule::removePriorityShortname(const String& shortname)
{
    if (shortname.isEmpty()) {
        return false;
    }
    
    for (auto it = priorityShortnames.begin(); it != priorityShortnames.end(); ++it) {
        if (it->equalsIgnoreCase(shortname)) {
            priorityShortnames.erase(it);
            LOG_INFO("ARR: Removed priority node: %s (%d remaining)", shortname.c_str(), priorityShortnames.size());
            return true;
        }
    }
    return false;
}

void ARRModule::clearPriorityShortnames()
{
    size_t count = priorityShortnames.size();
    priorityShortnames.clear();
    LOG_INFO("ARR: Cleared %d priority nodes", count);
}

size_t ARRModule::getPriorityNodeCount() const
{
    return priorityShortnames.size();
}

void ARRModule::enableStatusBroadcast(bool enabled)
{
    statusBroadcastEnabled = enabled;
}

bool ARRModule::isStatusBroadcastEnabled() const
{
    return statusBroadcastEnabled;
}

void ARRModule::broadcastStatusMessage(const char* message)
{
    if (!statusBroadcastEnabled || !service || !router || !message) {
        return;
    }
    
    // Rate limiting: don't spam the channel
    uint32_t now = millis();
    if (now - lastStatusBroadcast < STATUS_BROADCAST_COOLDOWN) {
        return;
    }
    lastStatusBroadcast = now;
    
    // Allocate packet for sending
    meshtastic_MeshPacket *p = router->allocForSending();
    if (!p) {
        LOG_WARN("ARR: Failed to allocate packet for status broadcast");
        return;
    }
    
    // Configure as text message
    p->decoded.portnum = meshtastic_PortNum_TEXT_MESSAGE_APP;
    p->to = NODENUM_BROADCAST;  // Broadcast to all
    p->channel = statusChannelIndex;  // Send on arr status channel
    p->want_ack = false;  // Don't need acks for status messages
    p->priority = meshtastic_MeshPacket_Priority_BACKGROUND;  // Low priority
    
    // Copy message to payload with bounds checking
    size_t len = strlen(message);
    size_t maxPayload = sizeof(p->decoded.payload.bytes) - 1;
    if (len > maxPayload) {
        len = maxPayload;  // Truncate if too long
        LOG_WARN("ARR: Status message truncated from %d to %d bytes", strlen(message), len);
    }
    memcpy(p->decoded.payload.bytes, message, len);
    p->decoded.payload.size = len;
    
    // Send the message
    service->sendToMesh(p, RX_SRC_USER);
}

void ARRModule::broadcastRoleChange(bool wasMuted, bool nowMuted, const char* reason)
{
    if (!statusBroadcastEnabled || !reason) {
        return;
    }
    
    // Get node info for creating the message
    const meshtastic_NodeInfoLite *myNode = nodeDB ? nodeDB->getMeshNodeByIndex(0) : nullptr;  // Our own node
    char nodeId[MAX_NODE_NAME_BUFFER] = "Unknown";
    if (myNode && myNode->has_user && myNode->user.short_name[0] != '\0') {
        strncpy(nodeId, myNode->user.short_name, sizeof(nodeId) - 1);
        nodeId[sizeof(nodeId) - 1] = '\0';  // Ensure null termination
    }
    
    // Create role change message with safe formatting
    char message[MAX_MESSAGE_SIZE];
    int result = snprintf(message, sizeof(message), "ARR %s: %s→%s (%s)", 
                         nodeId,
                         wasMuted ? "MUTE" : "CLIENT",
                         nowMuted ? "MUTE" : "CLIENT",
                         reason);
    
    if (result < 0 || result >= (int)sizeof(message)) {
        LOG_WARN("ARR: Role change message formatting error or truncation");
        // Use a fallback message
        snprintf(message, sizeof(message), "ARR: Role change %s→%s", 
                wasMuted ? "MUTE" : "CLIENT", nowMuted ? "MUTE" : "CLIENT");
    }
    
    broadcastStatusMessage(message);
}

void ARRModule::broadcastComprehensiveStatus()
{
    if (!statusBroadcastEnabled || !service || !router) {
        return;
    }
    
    // Build comprehensive status message using stack allocation to avoid heap fragmentation
    char message[MAX_MESSAGE_SIZE];
    char priorityInfo[MAX_STATUS_BUFFER];
    char routerInfo[MAX_STATUS_BUFFER];
    
    formatPriorityNodeStatus(priorityInfo, sizeof(priorityInfo));
    formatRouterAnalysis(routerInfo, sizeof(routerInfo));
    
    const char* currentRole = getCurrentMuteState() ? "MUTE" : "CLIENT";
    
    int result = snprintf(message, sizeof(message), "ARR: %s | %s | %s", 
                         currentRole, routerInfo, priorityInfo);
    
    if (result < 0 || result >= (int)sizeof(message)) {
        LOG_WARN("ARR: Comprehensive status message formatting error or truncation");
        // Use a simpler fallback message
        snprintf(message, sizeof(message), "ARR: %s | %d priority nodes", 
                currentRole, (int)priorityShortnames.size());
    }
    
    broadcastStatusMessage(message);
}

void ARRModule::formatPriorityNodeStatus(char* buffer, size_t bufSize)
{
    if (!buffer || bufSize == 0) {
        return;
    }
    
    if (priorityShortnames.empty()) {
        snprintf(buffer, bufSize, "No priority nodes");
        return;
    }
    
    if (!nodeDB || !nodeDB->meshNodes) {
        snprintf(buffer, bufSize, "NodeDB unavailable");
        return;
    }
    
    // Use local char arrays instead of String to avoid heap allocation
    char activeNodes[MAX_STATUS_BUFFER] = "";
    char debugInfo[MAX_STATUS_BUFFER] = "";
    int activeCount = 0;
    int foundCount = 0;
    size_t activeNodesLen = 0;
    size_t debugInfoLen = 0;
    
    for (const auto& shortname : priorityShortnames) {
        for (int i = 0; i < nodeDB->numMeshNodes; i++) {
            const meshtastic_NodeInfoLite *node = &nodeDB->meshNodes->at(i);
            if (!node || !node->has_user) continue;
            
            if (strcasecmp(node->user.short_name, shortname.c_str()) == 0) {
                foundCount++;
                uint32_t ageSeconds = getNodeAge(node);
                
                // Check if active and strong enough for override using improved reliability check
                if (isNodeDataReliable(node) && 
                    node->snr >= STRONG_SIGNAL_OVERRIDE_SNR &&
                    !node->via_mqtt && 
                    (!node->has_hops_away || node->hops_away == 0)) {
                    
                    // Format active node info
                    char nodeInfo[50];
                    snprintf(nodeInfo, sizeof(nodeInfo), "%s%s(%.1fdB)",
                            activeCount > 0 ? "," : "",
                            shortname.c_str(),
                            node->snr);
                    
                    if (activeNodesLen + strlen(nodeInfo) < sizeof(activeNodes) - 1) {
                        strcat(activeNodes, nodeInfo);
                        activeNodesLen += strlen(nodeInfo);
                    }
                    activeCount++;
                } else {
                    // Format debug info for inactive nodes
                    char nodeDebug[80]; // Increased buffer size for time quality info
                    snprintf(nodeDebug, sizeof(nodeDebug), "%s%s(%.1fdB",
                            foundCount > 1 ? "," : "",
                            shortname.c_str(),
                            node->snr);
                    
                    if (ageSeconds > STRONG_SIGNAL_TIMEOUT_SEC) strcat(nodeDebug, ",old");
                    if (node->snr < STRONG_SIGNAL_OVERRIDE_SNR) strcat(nodeDebug, ",weak");
                    if (node->via_mqtt) strcat(nodeDebug, ",mqtt");
                    if (node->has_hops_away && node->hops_away > 0) strcat(nodeDebug, ",multi-hop");
                    
                    // Add time quality info for debugging
                    RTCQuality quality = getRTCQuality();
                    if (quality < RTCQualityFromNet) {
                        strcat(nodeDebug, ",poor-time");
                    }
                    strcat(nodeDebug, ")");
                    
                    if (debugInfoLen + strlen(nodeDebug) < sizeof(debugInfo) - 1) {
                        strcat(debugInfo, nodeDebug);
                        debugInfoLen += strlen(nodeDebug);
                    }
                }
                break;
            }
        }
    }
    
    if (activeCount > 0) {
        snprintf(buffer, bufSize, "Priority: %s", activeNodes);
    } else if (foundCount > 0) {
        // Show debug info for found but inactive nodes
        snprintf(buffer, bufSize, "Priority: %s (inactive)", debugInfo);
    } else {
        snprintf(buffer, bufSize, "Priority: none found");
    }
}

void ARRModule::formatRouterAnalysis(char* buffer, size_t bufSize)
{
    // Refresh cache if needed
    if (nodeDB->meshNodes->size() != lastNodeDBUpdate) {
        refreshRouterCache();
    }
    
    snprintf(buffer, bufSize, "Routers: %d", 
             totalRouterCount);
}

void ARRModule::requestNodeInfoFromStalePriorityNodes()
{
    if (!nodeInfoModule || !nodeDB) {
        return;
    }
    
    if (priorityShortnames.empty()) {
        return;
    }
    
    uint32_t now = millis();
    
    // Look for stale priority nodes and request fresh info
    for (const auto& shortname : priorityShortnames) {
        for (int i = 0; i < nodeDB->numMeshNodes; i++) {
            auto node = nodeDB->getMeshNodeByIndex(i);
            if (!node || !node->has_user) continue;
            
            if (strcasecmp(node->user.short_name, shortname.c_str()) == 0) {
                uint32_t timeSinceHeard = getNodeAge(node);
                
                // If node data is getting stale but not ancient, and we haven't requested info recently
                if (shouldRequestNodeInfo(node->num, timeSinceHeard)) {
                    // Send NodeInfo request with want_response=true to get fresh data
                    nodeInfoModule->sendOurNodeInfo(node->num, true, 0, true); // shorterTimeout=true
                    
                    // Record when we made this request
                    lastNodeInfoRequest[node->num] = now;
                    LOG_DEBUG("ARR: Requested fresh info for priority node: %s", shortname.c_str());
                }
                break;
            }
        }
    }
}

bool ARRModule::shouldRequestNodeInfo(NodeNum nodeId, uint32_t timeSinceHeard)
{
    uint32_t now = millis();
    RTCQuality timeQuality = getRTCQuality();
    
    // Adjust staleness thresholds based on time quality
    uint32_t minStaleAge = STALE_NODE_MIN_AGE;
    uint32_t maxStaleAge = STALE_NODE_MAX_AGE;
    
    // With poor time quality, be more aggressive about refreshing node info
    if (timeQuality < RTCQualityFromNet) {
        minStaleAge = STALE_NODE_MIN_AGE / 2;  // 15 minutes instead of 30
        maxStaleAge = STALE_NODE_MAX_AGE / 2;  // 1 hour instead of 2
        LOG_DEBUG("ARR: Using shorter staleness intervals due to poor time quality: %s", RtcName(timeQuality));
    }
    
    // Only request if:
    // 1. Node data is getting stale (adjustable based on time quality)
    // 2. We haven't requested info from this node recently (2 minute cooldown)
    // 3. Node data isn't completely ancient (don't spam old nodes)
    
    bool dataIsGettingStale = (timeSinceHeard > minStaleAge) && (timeSinceHeard < maxStaleAge);
    
    auto lastRequestIt = lastNodeInfoRequest.find(nodeId);
    bool requestCooldownExpired = (lastRequestIt == lastNodeInfoRequest.end()) || 
                                  ((now - lastRequestIt->second) > NODE_INFO_REQUEST_COOLDOWN);
    
    return dataIsGettingStale && requestCooldownExpired;
}