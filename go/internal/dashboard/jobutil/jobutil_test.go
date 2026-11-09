package jobutil

import (
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// The constants-drift guard against the agent job module lands with the job
// module itself; it asserts RayInternalNamespacePrefix and JobIDMetadataKey
// stay equal to the agent-side constants.

func TestPtrString(t *testing.T) {
	assert.Equal(t, "x", *PtrString("x"))
}

func TestInt64Ptr(t *testing.T) {
	assert.Equal(t, int64(7), *Int64Ptr(7))
}

func TestRuntimeEnvToDict(t *testing.T) {
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvToDict(""))
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvToDict("not-json"))
	got := RuntimeEnvToDict(`{"pip":["x"],"n":2}`)
	assert.Equal(t, map[string]interface{}{"pip": []interface{}{"x"}, "n": 2.0}, got)
}

func TestRuntimeEnvSerialized(t *testing.T) {
	entry := &proto.JobTableData{
		Config: &proto.JobConfig{
			RuntimeEnvInfo: &proto.RuntimeEnvInfo{SerializedRuntimeEnv: `{"a":1}`},
		},
	}
	assert.Equal(t, `{"a":1}`, RuntimeEnvSerialized(entry))
}

func TestRuntimeEnvFromJSON(t *testing.T) {
	assert.Nil(t, RuntimeEnvFromJSON(nil))
	empty := "{}"
	assert.Equal(t, map[string]interface{}{}, RuntimeEnvFromJSON(&empty))
	val := `{"k":"v"}`
	got := RuntimeEnvFromJSON(&val)
	assert.Equal(t, map[string]interface{}{"k": "v"}, got)
}

func TestDriverIP(t *testing.T) {
	// DriverAddress takes precedence.
	entry := &proto.JobTableData{
		DriverIpAddress: "1.1.1.1",
		DriverAddress:   &proto.Address{IpAddress: "2.2.2.2"},
	}
	assert.Equal(t, "2.2.2.2", DriverIP(entry))

	// Falls back to DriverIpAddress when DriverAddress is empty/nil.
	entry2 := &proto.JobTableData{DriverIpAddress: "3.3.3.3"}
	assert.Equal(t, "3.3.3.3", DriverIP(entry2))

	entry3 := &proto.JobTableData{DriverAddress: &proto.Address{}}
	assert.Equal(t, "", DriverIP(entry3))

	assert.Equal(t, "", DriverIP(&proto.JobTableData{}))
}
