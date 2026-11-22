package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/ER-brocode/turbostorm-load/pkg/generator"
	"github.com/ER-brocode/turbostorm-load/pkg/metrics"
)

// NodeRole represents whether this coordinator node is master or follower.
type NodeRole string

const (
	RoleMaster   NodeRole = "master"
	RoleFollower NodeRole = "follower"
)

// NodeStatus tracks an individual worker node state in the cluster.
type NodeStatus struct {
	ID        string                 `json:"id"`
	Address   string                 `json:"address"`
	Role      NodeRole               `json:"role"`
	State     string                 `json:"state"` // "idle", "ramping", "steady", "stopped"
	TargetRPS float64                `json:"target_rps"`
	Metrics   metrics.MetricsSummary `json:"metrics"`
	LastSeen  time.Time              `json:"last_seen"`
}

// ClusterPlan coordinates multi-node traffic ramps with synchronous start timestamps.
type ClusterPlan struct {
	TestID       string                    `json:"test_id"`
	SyncStartAt  time.Time                 `json:"sync_start_at"`
	Config       generator.GeneratorConfig `json:"config"`
	PerNodeAlloc map[string]float64        `json:"per_node_alloc"` // NodeID -> target RPS
}

// Coordinator orchestrates distributed load generation nodes.
type Coordinator struct {
	mu           sync.RWMutex
	id           string
	role         NodeRole
	bindAddr     string
	masterAddr   string
	nodes        map[string]*NodeStatus
	activeGen    *generator.Generator
	server       *http.Server
	stopCh       chan struct{}
	globalAgg    *metrics.Aggregator
	plan         *ClusterPlan
}

// NewCoordinator creates a coordinator instance.
func NewCoordinator(id string, role NodeRole, bindAddr, masterAddr string) *Coordinator {
	return &Coordinator{
		id:         id,
		role:       role,
		bindAddr:   bindAddr,
		masterAddr: masterAddr,
		nodes:      make(map[string]*NodeStatus),
		stopCh:     make(chan struct{}),
		globalAgg:  metrics.NewAggregator(),
	}
}

// RegisterNode adds or updates a follower node.
func (c *Coordinator) RegisterNode(node *NodeStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node.LastSeen = time.Now()
	c.nodes[node.ID] = node
}

// PrepareClusterPlan splits overall target RPS equally among active cluster nodes.
func (c *Coordinator) PrepareClusterPlan(testID string, cfg generator.GeneratorConfig) (*ClusterPlan, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	activeCount := len(c.nodes)
	if activeCount == 0 {
		activeCount = 1
	}

	alloc := make(map[string]float64)
	splitRPS := cfg.TargetRPS / float64(activeCount)
	for id := range c.nodes {
		alloc[id] = splitRPS
	}

	plan := &ClusterPlan{
		TestID:       testID,
		SyncStartAt:  time.Now().Add(2 * time.Second), // Synchronized T0 ramp start
		Config:       cfg,
		PerNodeAlloc: alloc,
	}
	c.plan = plan
	return plan, nil
}

// StartHTTP starts coordinator internal API server.
func (c *Coordinator) StartHTTP() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
			"node":   c.id,
			"role":   string(c.role),
		})
	})

	mux.HandleFunc("/api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(c.nodes)
	})

	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		gen := c.activeGen
		c.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		if gen != nil {
			_ = json.NewEncoder(w).Encode(gen.Report())
		} else {
			_ = json.NewEncoder(w).Encode(c.globalAgg.Summary())
		}
	})

	mux.HandleFunc("/api/v1/dispatch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var plan ClusterPlan
		if err := json.NewDecoder(r.Body).Decode(&plan); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Schedule local start at synchronized time
		go c.executePlan(plan)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "dispatched"})
	})

	c.server = &http.Server{
		Addr:    c.bindAddr,
		Handler: mux,
	}

	go func() {
		_ = c.server.ListenAndServe()
	}()

	return nil
}

func (c *Coordinator) executePlan(plan ClusterPlan) {
	timeToWait := time.Until(plan.SyncStartAt)
	if timeToWait > 0 {
		time.Sleep(timeToWait)
	}

	// Adjust node configuration with allocated share
	nodeCfg := plan.Config
	if alloc, exists := plan.PerNodeAlloc[c.id]; exists {
		nodeCfg.TargetRPS = alloc
	}

	gen := generator.NewGenerator(nodeCfg)
	c.mu.Lock()
	c.activeGen = gen
	c.mu.Unlock()

	_ = gen.Start(context.Background())
	gen.Wait()
}

// Stop terminates active generator and server.
func (c *Coordinator) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeGen != nil {
		c.activeGen.Stop()
	}
	if c.server != nil {
		return c.server.Close()
	}
	return nil
}

// GetNodes returns copy of active registered nodes.
func (c *Coordinator) GetNodes() map[string]NodeStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make(map[string]NodeStatus, len(c.nodes))
	for k, v := range c.nodes {
		res[k] = *v
	}
	return res
}
