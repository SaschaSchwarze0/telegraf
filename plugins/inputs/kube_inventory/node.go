package kube_inventory

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"github.com/influxdata/telegraf"
)

func collectNodes(ctx context.Context, acc telegraf.Accumulator, ki *KubernetesInventory) {
	list, err := ki.client.getNodes(ctx)
	if err != nil {
		acc.AddError(err)
		return
	}

	ki.gatherNodeCount(len(list.Items), acc)

	for _, n := range list.Items {
		ki.gatherNode(n, acc)
	}
}

func (ki *KubernetesInventory) gatherNodeCount(count int, acc telegraf.Accumulator) {
	fields := map[string]interface{}{"node_count": count}
	tags := map[string]string{}

	acc.AddFields(nodeMeasurement, fields, tags)
}

func (ki *KubernetesInventory) gatherNode(n corev1.Node, acc telegraf.Accumulator) {
	fields := map[string]interface{}{}
	tags := map[string]string{
		"node_name":         n.Name,
		"cluster_namespace": n.Annotations["cluster.x-k8s.io/cluster-namespace"],
		"version":           n.Status.NodeInfo.KubeletVersion,
	}

	for resourceName, val := range n.Status.Capacity {
		switch resourceName {
		case "cpu":
			fields["capacity_cpu_cores"] = ki.convertQuantity(val.String(), 1)
			fields["capacity_millicpu_cores"] = ki.convertQuantity(val.String(), 1000)
		case "ephemeral-storage":
			fields["capacity_ephemeral_storage_bytes"] = ki.convertQuantity(val.String(), 1)
		case "memory":
			fields["capacity_memory_bytes"] = ki.convertQuantity(val.String(), 1)
		case "pods":
			fields["capacity_pods"] = atoi(val.String())
		}
	}

	for resourceName, val := range n.Status.Allocatable {
		switch resourceName {
		case "cpu":
			fields["allocatable_cpu_cores"] = ki.convertQuantity(val.String(), 1)
			fields["allocatable_millicpu_cores"] = ki.convertQuantity(val.String(), 1000)
		case "ephemeral-storage":
			fields["allocatable_ephemeral_storage_bytes"] = ki.convertQuantity(val.String(), 1)
		case "memory":
			fields["allocatable_memory_bytes"] = ki.convertQuantity(val.String(), 1)
		case "pods":
			fields["allocatable_pods"] = atoi(val.String())
		}
	}

	fields["spec_unschedulable"] = n.Spec.Unschedulable
	fields["condition_disk_pressure"] = false
	fields["condition_memory_pressure"] = false
	fields["condition_network_available"] = false
	fields["condition_pid_pressure"] = false
	fields["condition_ready"] = false

	for _, condition := range n.Status.Conditions {
		switch condition.Type {
		case corev1.NodeDiskPressure:
			if condition.Status == corev1.ConditionTrue {
				fields["condition_disk_pressure"] = true
			}
		case corev1.NodeMemoryPressure:
			if condition.Status == corev1.ConditionTrue {
				fields["condition_memory_pressure"] = true
			}
		case corev1.NodeNetworkUnavailable:
			if condition.Status == corev1.ConditionFalse {
				fields["condition_network_available"] = true
			}
		case corev1.NodePIDPressure:
			if condition.Status == corev1.ConditionTrue {
				fields["condition_pid_pressure"] = true
			}
		case corev1.NodeReady:
			if condition.Status == corev1.ConditionTrue {
				fields["condition_ready"] = true
			}
		}
	}

	acc.AddFields(nodeMeasurement, fields, tags)
}
