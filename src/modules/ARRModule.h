#pragma once
#include "SinglePortModule.h"
#include "configuration.h"
#include "mesh/generated/meshtastic/module_config.pb.h"
#include <map>
#include <vector>

/**
 * ARR (like a pirate) - EXPERIMENTAL
 *
 * Automatically Revolving Roles (Automatic Role Regulation)
 * Switches between CLIENT and CLIENT_MUTE based on
 * priority nodes with strong SNR on direct links.
 * 
 * WARNING: This module is experimental and needs further development.
 * Use with caution in production environments.
 * 
 * Logic:
 * - MUTE when any priority node has SNR ≥ STRONG_SIGNAL_OVERRIDE_SNR on direct link
 * - CLIENT when no priority nodes meet criteria
 * 
 * Key Features:
 * - Configurable priority node lists (currently hardcoded for testing)
 * - Direct link validation (excludes MQTT and multi-hop)
 * - Rate limiting and cooldown periods
 * - Status broadcasting on designated channel
 * - Automatic node info refresh for stale priority nodes
 * 
 * Thread Safety:
 * - Inherits from OSThread for periodic execution
 * - Uses atomic state transitions for role changes
 * - Rate-limited status updates to prevent flooding
 */
class ARRModule : public SinglePortModule, private concurrency::OSThread
{
private:
    // Simple router info structure
    struct RouterInfo {
        NodeNum nodeId;         // Node identifier
        float snr;              // Signal-to-noise ratio (-20 to +10 dB)
    };

    // Timing constants - EXPERIMENTAL/TEST MODE 
    static constexpr uint32_t EVALUATION_INTERVAL = 60 * 1000;        // 1 minute - routine checks
    static constexpr uint32_t MIN_CHANGE_INTERVAL = 60 * 1000;        // 1 minute - minimum between role changes
    static constexpr uint32_t FAST_CHECK_INTERVAL = 30 * 1000;        // 30 seconds - after network changes

    // Strong signal override thresholds
    static constexpr float STRONG_SIGNAL_OVERRIDE_SNR = 5.0f;  // SNR > 5dB triggers override
    static constexpr float STRONG_SIGNAL_TIMEOUT_SEC = 3600;   // 1 hour
    static constexpr uint32_t ROUTER_TIMEOUT_SEC = 900; // 15 minutes
    
    // Configuration limits
    static constexpr size_t MAX_PRIORITY_SHORTNAMES = 20;             // Maximum number of priority nodes
    static constexpr size_t MAX_SHORTNAME_LENGTH = 16;                // Maximum shortname length
    
    // Performance cache
    uint32_t lastNodeDBUpdate = 0;
    std::vector<RouterInfo> cachedRouters;
    int totalRouterCount = 0;

    // State management 
    uint32_t lastRoleChange = 0;
    uint32_t lastEvaluation = 0;
    bool networkIsStable = true;
    uint32_t evaluationCount = 0;
    
    // Strong signal override state
    std::vector<String> priorityShortnames;
    uint32_t lastStrongSignalOverride = 0;
    bool strongSignalOverrideActive = false;
    
    // Priority node refresh tracking
    std::map<NodeNum, uint32_t> lastNodeInfoRequest; // Track when we last requested info from each priority node
    static constexpr uint32_t NODE_INFO_REQUEST_COOLDOWN = 2 * 60 * 1000; // 2 minutes between requests per node
    static constexpr uint32_t STALE_NODE_MIN_AGE = 30 * 60;              // 30 minutes - minimum age to consider stale
    static constexpr uint32_t STALE_NODE_MAX_AGE = 2 * 60 * 60;          // 2 hours - maximum age before giving up

    // Status broadcasting configuration 
    bool statusBroadcastEnabled = true;  // ON by default
    uint8_t statusChannelIndex = 1;      // Channel 1 for ARR status messages
    uint32_t lastStatusBroadcast = 0;
    uint32_t lastPeriodicBroadcast = 0;
    static constexpr uint32_t STATUS_BROADCAST_COOLDOWN = 30 * 1000; // 30 seconds between broadcasts
    static constexpr uint32_t PERIODIC_BROADCAST_INTERVAL = 5 * 60 * 1000; // 5 minutes
    
    // Buffer sizes for message formatting
    static constexpr size_t MAX_MESSAGE_SIZE = 240;                   // Maximum message buffer size
    static constexpr size_t MAX_STATUS_BUFFER = 100;                  // Status info buffer size
    static constexpr size_t MAX_NODE_NAME_BUFFER = 20;                // Node name display buffer

    // Anti-cascade protection
    bool shouldDelayRoleChange() const;

    // Router analysis
    void refreshRouterCache();
    bool isValidRouter(const meshtastic_NodeInfoLite *node) const;

    // Decision logic
    bool checkStrongSignalOverride();
    bool getCurrentMuteState() const;
    void applyMuteDecision(bool shouldMute, const char* reason);
    uint32_t getNextCheckInterval();
    
    // Priority node refresh
    void requestNodeInfoFromStalePriorityNodes();
    bool shouldRequestNodeInfo(NodeNum nodeId, uint32_t timeSinceHeard);

    // Node data reliability assessment
    bool isNodeDataReliable(const meshtastic_NodeInfoLite *node) const;
    uint32_t getNodeAge(const meshtastic_NodeInfoLite *node) const;
    
    // Status broadcasting
    void broadcastStatusMessage(const char* message);
    void broadcastRoleChange(bool wasMuted, bool nowMuted, const char* reason);
    void broadcastComprehensiveStatus();
    void formatPriorityNodeStatus(char* buffer, size_t bufSize);
    void formatRouterAnalysis(char* buffer, size_t bufSize);

public:
    ARRModule();
    
    // Public state access
    bool moduleEnabled = false;

    /**
     * Priority node management - controls which nodes can trigger role changes
     * @param shortname The short name of the priority node (max 16 chars)
     * @return true if successful, false if invalid or limit reached
     */
    bool addPriorityShortname(const String& shortname);
    
    /**
     * Remove a priority node from the list
     * @param shortname The short name to remove
     * @return true if found and removed, false if not found
     */
    bool removePriorityShortname(const String& shortname);
    
    /**
     * Clear all priority nodes from the list
     */
    void clearPriorityShortnames();
    
    /**
     * Get the current number of configured priority nodes
     * @return Number of priority nodes
     */
    size_t getPriorityNodeCount() const;

    /**
     * Enable or disable status broadcasting
     * @param enabled Whether to broadcast status messages
     */
    void enableStatusBroadcast(bool enabled);
    
    /**
     * Check if status broadcasting is enabled
     * @return true if enabled
     */
    bool isStatusBroadcastEnabled() const;

protected:
    virtual int32_t runOnce() override;
};

extern ARRModule *arrModule;