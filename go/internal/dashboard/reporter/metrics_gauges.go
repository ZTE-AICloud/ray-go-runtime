package reporter

// Prometheus label key sets, aligned with python/ray/dashboard/consts.py
// (NODE_TAG_KEYS / GPU_TAG_KEYS / TPU_TAG_KEYS / CLUSTER_TAG_KEYS /
// COMPONENT_METRICS_TAG_KEYS / COMPONENT_GPU_TAG_KEYS). They are the label
// names a gauge declares, in declaration order.
var (
	nodeTagKeys      = []string{"ip", "Version", "SessionName", "IsHeadNode", "RayNodeType"}
	gpuTagKeys       = append(append([]string{}, nodeTagKeys...), "GpuDeviceName", "GpuIndex")
	tpuTagKeys       = append(append([]string{}, nodeTagKeys...), "TpuDeviceName", "TpuIndex", "TpuType", "TpuTopology")
	clusterTagKeys   = []string{"node_type", "Version", "SessionName"}
	componentTagKeys = []string{"ip", "pid", "Version", "Component", "SessionName"}
	// componentGPUTagKeys labels the two component GPU gauges. Python declares
	// COMPONENT_GPU_TAG_KEYS = GPU_TAG_KEYS + COMPONENT_METRICS_TAG_KEYS for
	// them, but that concatenation repeats "Version" and "SessionName", which a
	// Prometheus label set cannot represent (label names must be unique), and
	// the reporter never attaches the node/GPU tags to component records: its
	// _generate_system_stats_record tags are {ip, Component[, pid]} merged with
	// the global {Version, SessionName}. Declaring COMPONENT_METRICS_TAG_KEYS
	// therefore describes exactly the label set Python emits for these gauges
	// instead of adding permanently empty node/GPU labels.
	componentGPUTagKeys = componentTagKeys
)

// gaugeDef describes one Prometheus gauge, aligned with a Python Gauge entry of
// METRICS_GAUGES (reporter_agent.py:124-381). Help is the Python description.
type gaugeDef struct {
	Name       string
	Help       string
	LabelNames []string
}

// Gauge names of the 21 node-level metrics, aligned with the METRICS_GAUGES
// keys in reporter_agent.py.
const (
	GaugeNodeCPUUtilization      = "node_cpu_utilization"
	GaugeNodeCPUCount            = "node_cpu_count"
	GaugeNodeMemUsed             = "node_mem_used"
	GaugeNodeMemAvailable        = "node_mem_available"
	GaugeNodeMemTotal            = "node_mem_total"
	GaugeNodeMemSharedBytes      = "node_mem_shared_bytes"
	GaugeNodeDiskIORead          = "node_disk_io_read"
	GaugeNodeDiskIOWrite         = "node_disk_io_write"
	GaugeNodeDiskIOReadCount     = "node_disk_io_read_count"
	GaugeNodeDiskIOWriteCount    = "node_disk_io_write_count"
	GaugeNodeDiskIOReadSpeed     = "node_disk_io_read_speed"
	GaugeNodeDiskIOWriteSpeed    = "node_disk_io_write_speed"
	GaugeNodeDiskReadIOPS        = "node_disk_read_iops"
	GaugeNodeDiskWriteIOPS       = "node_disk_write_iops"
	GaugeNodeDiskUsage           = "node_disk_usage"
	GaugeNodeDiskFree            = "node_disk_free"
	GaugeNodeDiskUtilizationPct  = "node_disk_utilization_percentage"
	GaugeNodeNetworkSent         = "node_network_sent"
	GaugeNodeNetworkReceived     = "node_network_received"
	GaugeNodeNetworkSendSpeed    = "node_network_send_speed"
	GaugeNodeNetworkReceiveSpeed = "node_network_receive_speed"
)

// Gauge names of the 4 GPU-level and 5 TPU-level metrics.
const (
	GaugeNodeGPUsAvailable   = "node_gpus_available"
	GaugeNodeGPUsUtilization = "node_gpus_utilization"
	GaugeNodeGRAMUsed        = "node_gram_used"
	GaugeNodeGRAMAvaliable   = "node_gram_available"

	GaugeTpuTensorcoreUtilization = "tpu_tensorcore_utilization"
	GaugeTpuMemoryBandwidth       = "tpu_memory_bandwidth_utilization"
	GaugeTpuDutyCycle             = "tpu_duty_cycle"
	GaugeTpuMemoryUsed            = "tpu_memory_used"
	GaugeTpuMemoryTotal           = "tpu_memory_total"
)

// Gauge names of the 3 cluster-level metrics.
const (
	GaugeClusterActiveNodes  = "cluster_active_nodes"
	GaugeClusterFailedNodes  = "cluster_failed_nodes"
	GaugeClusterPendingNodes = "cluster_pending_nodes"
)

// metricsGauges is the full list of gauges recorded and exported, aligned
// one-to-one with the Python METRICS_GAUGES mapping (name, description, label
// keys). Keeping the description text identical to Python means the exported
// Prometheus HELP lines match too.
var metricsGauges = []gaugeDef{
	// CPU metrics.
	{GaugeNodeCPUUtilization, "Total CPU usage on a ray node", nodeTagKeys},
	{GaugeNodeCPUCount, "Total CPUs available on a ray node", nodeTagKeys},
	// Memory metrics.
	{GaugeNodeMemUsed, "Memory usage on a ray node", nodeTagKeys},
	{GaugeNodeMemAvailable, "Memory available on a ray node", nodeTagKeys},
	{GaugeNodeMemTotal, "Total memory on a ray node", nodeTagKeys},
	{GaugeNodeMemSharedBytes, "Total shared memory usage on a ray node", nodeTagKeys},
	// GPU metrics.
	{GaugeNodeGPUsAvailable, "Total GPUs available on a ray node", gpuTagKeys},
	{GaugeNodeGPUsUtilization, "Total GPUs usage on a ray node", gpuTagKeys},
	{GaugeNodeGRAMUsed, "Total GPU RAM usage on a ray node", gpuTagKeys},
	{GaugeNodeGRAMAvaliable, "Total GPU RAM available on a ray node", gpuTagKeys},
	// TPU metrics.
	{GaugeTpuTensorcoreUtilization, "Percentage TPU tensorcore utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuMemoryBandwidth, "Percentage TPU memory bandwidth utilization on a ray node, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuDutyCycle, "Percentage of time during which the TPU was actively processing, value should be between 0 and 100", tpuTagKeys},
	{GaugeTpuMemoryUsed, "Total memory used by the accelerator in bytes", tpuTagKeys},
	{GaugeTpuMemoryTotal, "Total memory allocatable by the accelerator in bytes", tpuTagKeys},
	// Disk I/O metrics.
	{GaugeNodeDiskIORead, "Total read from disk", nodeTagKeys},
	{GaugeNodeDiskIOWrite, "Total written to disk", nodeTagKeys},
	{GaugeNodeDiskIOReadCount, "Total read ops from disk", nodeTagKeys},
	{GaugeNodeDiskIOWriteCount, "Total write ops to disk", nodeTagKeys},
	{GaugeNodeDiskIOReadSpeed, "Disk read speed", nodeTagKeys},
	{GaugeNodeDiskIOWriteSpeed, "Disk write speed", nodeTagKeys},
	{GaugeNodeDiskReadIOPS, "Disk read iops", nodeTagKeys},
	{GaugeNodeDiskWriteIOPS, "Disk write iops", nodeTagKeys},
	// Disk usage metrics.
	{GaugeNodeDiskUsage, "Total disk usage (bytes) on a ray node", nodeTagKeys},
	{GaugeNodeDiskFree, "Total disk free (bytes) on a ray node", nodeTagKeys},
	{GaugeNodeDiskUtilizationPct, "Total disk utilization (percentage) on a ray node", nodeTagKeys},
	// Network metrics.
	{GaugeNodeNetworkSent, "Total network sent", nodeTagKeys},
	{GaugeNodeNetworkReceived, "Total network received", nodeTagKeys},
	{GaugeNodeNetworkSendSpeed, "Network send speed", nodeTagKeys},
	{GaugeNodeNetworkReceiveSpeed, "Network receive speed", nodeTagKeys},
	// Component metrics.
	{ComponentGaugeCPUPercentage, "Total CPU usage of the components on a node.", componentTagKeys},
	{ComponentGaugeMemSharedBytes, "SHM usage of all components of the node. It is equivalent to the top command's SHR column.", componentTagKeys},
	{ComponentGaugeRSSMB, "RSS usage of all components on the node.", componentTagKeys},
	{ComponentGaugeUSSMB, "USS usage of all components on the node.", componentTagKeys},
	{ComponentGaugePSSMB, "PSS usage of all components on the node.", componentTagKeys},
	{ComponentGaugeNumFDs, "Number of open fds of all components on the node (Not available on Windows).", componentTagKeys},
	// Cluster metrics.
	{GaugeClusterActiveNodes, "Active nodes on the cluster", clusterTagKeys},
	{GaugeClusterFailedNodes, "Failed nodes on the cluster", clusterTagKeys},
	{GaugeClusterPendingNodes, "Pending nodes on the cluster", clusterTagKeys},
	// Component GPU metrics.
	{ComponentGaugeGPUPercentage, "GPU usage of all components on the node.", componentGPUTagKeys},
	{ComponentGaugeGPUMemoryMB, "GPU memory usage of all components on the node.", componentGPUTagKeys},
}
