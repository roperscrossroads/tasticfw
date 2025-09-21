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
        
        // Hardcoded priority shortnames for testing
        priorityShortnames.push_back("rxr1");
        priorityShortnames.push_back("CSR1");
        priorityShortnames.push_back("7e4c");
        priorityShortnames.push_back("WOLF");
        priorityShortnames.push_back("TCMQ");
        priorityShortnames.push_back("CSR5");
        priorityShortnames.push_back("CS11");
        
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
        return EVALUATION_INTERVAL; // Shouldn't happen, but safety check
    }

    uint32_t now = millis();
    
    // Only do full evaluation every EVALUATION_INTERVAL
    if (now - lastEvaluation < EVALUATION_INTERVAL) {
        return 30 * 1000; // Check again in 30 seconds
    }
    
    evaluationCount++;
    lastEvaluation = now;
    
    // Always respect minimum role change interval
    if (shouldDelayRoleChange()) {
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
    // Use validated time for priority override decisions to avoid false positives with bad clocks
    uint32_t now = getValidTime(RTCQualityDevice);
    if (now == 0) {
        return false; // Can't make reliable time-based decisions without valid time
    }
    
    if (priorityShortnames.empty()) {
        return false; 
    }
    bool hasStrongSignal = false;

    for (int i = 0; i < nodeDB->numMeshNodes; i++) {
        auto node = nodeDB->getMeshNodeByIndex(i);
        if (!node) continue;

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
            continue;
        }
        
        if (node->has_hops_away && node->hops_away > 0) {
            continue;
        }

        // Check if node is recently heard and has strong signal
        uint32_t timeSinceHeard = sinceLastSeen_fromTimestamp(node->last_heard);
        if (timeSinceHeard > STRONG_SIGNAL_TIMEOUT_SEC) {
            continue;
        }

        // Check SNR threshold - now we can trust it since it's a direct link
        if (node->snr >= STRONG_SIGNAL_OVERRIDE_SNR) {
            hasStrongSignal = true;
            lastStrongSignalOverride = now;
            break; // One strong signal is enough
        }
    }

    strongSignalOverrideActive = hasStrongSignal;
    return hasStrongSignal;
}

void ARRModule::refreshRouterCache()
{
    cachedRouters.clear();
    totalRouterCount = 0;
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

    // Must be recently active (15 minutes)
    if (sinceLastSeen_fromTimestamp(node->last_heard) > ROUTER_TIMEOUT_SEC) {
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

uint32_t ARRModule::sinceLastSeen_fromTimestamp(uint32_t timestamp) const
{
    uint32_t now = getTime(); // Use same time source as NodeDB's sinceLastSeen()
    
    int delta = (int)(now - timestamp);
    if (delta < 0) // Handle clock drift - same as NodeDB implementation
        delta = 0;
        
    return delta;
}

void ARRModule::addPriorityShortname(const String& shortname)
{
    // Check if already exists
    for (const auto& existing : priorityShortnames) {
        if (existing.equalsIgnoreCase(shortname)) {
            return;
        }
    }
    
    priorityShortnames.push_back(shortname);
}

void ARRModule::removePriorityShortname(const String& shortname)
{
    for (auto it = priorityShortnames.begin(); it != priorityShortnames.end(); ++it) {
        if (it->equalsIgnoreCase(shortname)) {
            priorityShortnames.erase(it);
            return;
        }
    }
}

void ARRModule::clearPriorityShortnames()
{
    priorityShortnames.clear();
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
    if (!statusBroadcastEnabled || !service || !router) {
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
        return;
    }
    
    // Configure as text message
    p->decoded.portnum = meshtastic_PortNum_TEXT_MESSAGE_APP;
    p->to = NODENUM_BROADCAST;  // Broadcast to all
    p->channel = statusChannelIndex;  // Send on arr status channel
    p->want_ack = false;  // Don't need acks for status messages
    p->priority = meshtastic_MeshPacket_Priority_BACKGROUND;  // Low priority
    
    // Copy message to payload
    size_t len = strlen(message);
    if (len > sizeof(p->decoded.payload.bytes) - 1) {
        len = sizeof(p->decoded.payload.bytes) - 1;  // Truncate if too long
    }
    memcpy(p->decoded.payload.bytes, message, len);
    p->decoded.payload.size = len;
    
    // Send the message
    service->sendToMesh(p, RX_SRC_USER);
}

void ARRModule::broadcastRoleChange(bool wasMuted, bool nowMuted, const char* reason)
{
    if (!statusBroadcastEnabled) {
        return;
    }
    
    // Get node info for creating the message
    const meshtastic_NodeInfoLite *myNode = nodeDB->getMeshNodeByIndex(0);  // Our own node
    String nodeId = "Unknown";
    if (myNode && myNode->has_user && myNode->user.short_name[0] != '\0') {
        nodeId = String(myNode->user.short_name);
    }
    
    // Create role change message
    char message[200];
    snprintf(message, sizeof(message), "ARR %s: %s→%s (%s)", 
             nodeId.c_str(),
             wasMuted ? "MUTE" : "CLIENT",
             nowMuted ? "MUTE" : "CLIENT",
             reason);
    
    broadcastStatusMessage(message);
}

void ARRModule::broadcastComprehensiveStatus()
{
    if (!statusBroadcastEnabled || !service || !router) {
        return;
    }
    
    // Build comprehensive status message
    char message[240];
    char priorityInfo[100];
    char routerInfo[100];
    
    formatPriorityNodeStatus(priorityInfo, sizeof(priorityInfo));
    formatRouterAnalysis(routerInfo, sizeof(routerInfo));
    
    const char* currentRole = getCurrentMuteState() ? "MUTE" : "CLIENT";
    
    snprintf(message, sizeof(message), "ARR: %s | %s | %s", 
             currentRole, routerInfo, priorityInfo);
    
    broadcastStatusMessage(message);
}

void ARRModule::formatPriorityNodeStatus(char* buffer, size_t bufSize)
{
    if (priorityShortnames.empty()) {
        snprintf(buffer, bufSize, "No priority nodes");
        return;
    }
    
    String activeNodes = "";
    String debugInfo = "";
    int activeCount = 0;
    int foundCount = 0;
    
    for (const auto& shortname : priorityShortnames) {
        for (int i = 0; i < nodeDB->numMeshNodes; i++) {
            const meshtastic_NodeInfoLite *node = &nodeDB->meshNodes->at(i);
            if (!node || !node->has_user) continue;
            
            if (strcasecmp(node->user.short_name, shortname.c_str()) == 0) {
                foundCount++;
                uint32_t ageSeconds = sinceLastSeen_fromTimestamp(node->last_heard);
                
                // Check if active and strong enough for override
                if (ageSeconds <= STRONG_SIGNAL_TIMEOUT_SEC && 
                    node->snr >= STRONG_SIGNAL_OVERRIDE_SNR &&
                    !node->via_mqtt && 
                    (!node->has_hops_away || node->hops_away == 0)) {
                    
                    if (activeCount > 0) activeNodes += ",";
                    activeNodes += shortname;
                    activeNodes += "(";
                    activeNodes += String(node->snr, 1);
                    activeNodes += "dB)";
                    activeCount++;
                } else {
                    // Add debug info for why it's not active
                    if (debugInfo.length() > 0) debugInfo += ",";
                    debugInfo += shortname;
                    debugInfo += "(";
                    debugInfo += String(node->snr, 1);
                    debugInfo += "dB";
                    if (ageSeconds > STRONG_SIGNAL_TIMEOUT_SEC) debugInfo += ",old";
                    if (node->snr < STRONG_SIGNAL_OVERRIDE_SNR) debugInfo += ",weak";
                    if (node->via_mqtt) debugInfo += ",mqtt";
                    if (node->has_hops_away && node->hops_away > 0) debugInfo += ",multi-hop";
                    debugInfo += ")";
                }
                break;
            }
        }
    }
    
    if (activeCount > 0) {
        snprintf(buffer, bufSize, "Priority: %s", activeNodes.c_str());
    } else if (foundCount > 0) {
        // Show debug info for found but inactive nodes
        snprintf(buffer, bufSize, "Priority: %s (inactive)", debugInfo.c_str());
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
    if (!nodeInfoModule) {
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
                uint32_t timeSinceHeard = sinceLastSeen_fromTimestamp(node->last_heard);
                
                // If node data is getting stale (more than 30 minutes old) but not ancient,
                // and we haven't requested info recently, send a NodeInfo request
                if (shouldRequestNodeInfo(node->num, timeSinceHeard)) {
                    // Send NodeInfo request with want_response=true to get fresh data
                    nodeInfoModule->sendOurNodeInfo(node->num, true, 0, true); // shorterTimeout=true
                    
                    // Record when we made this request
                    lastNodeInfoRequest[node->num] = now;
                }
                break;
            }
        }
    }
}

bool ARRModule::shouldRequestNodeInfo(NodeNum nodeId, uint32_t timeSinceHeard)
{
    uint32_t now = millis();
    
    // Only request if:
    // 1. Node data is getting stale (30 minutes to 2 hours old)
    // 2. We haven't requested info from this node recently (2 minute cooldown)
    // 3. Node data isn't completely ancient (don't spam old nodes)
    
    bool dataIsGettingStale = (timeSinceHeard > 30 * 60) && (timeSinceHeard < 2 * 60 * 60); // 30min - 2hr
    
    auto lastRequestIt = lastNodeInfoRequest.find(nodeId);
    bool requestCooldownExpired = (lastRequestIt == lastNodeInfoRequest.end()) || 
                                  ((now - lastRequestIt->second) > NODE_INFO_REQUEST_COOLDOWN);
    
    return dataIsGettingStale && requestCooldownExpired;
}