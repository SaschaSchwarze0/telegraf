package kube_inventory

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/influxdata/telegraf/plugins/common/tls"
)

type client struct {
	namespace     string
	timeout       time.Duration
	kubeClient    kubernetes.Interface
	metricsClient metrics.Interface
}

func newClient(baseURL, namespace, bearerTokenFile string, bearerToken string, timeout time.Duration, tlsConfig tls.ClientConfig) (*client, error) {
	config := &rest.Config{
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: tlsConfig.ServerName,
			Insecure:   tlsConfig.InsecureSkipVerify,
			CAFile:     tlsConfig.TLSCA,
			CertFile:   tlsConfig.TLSCert,
			KeyFile:    tlsConfig.TLSKey,
		},
		Host:          baseURL,
		ContentConfig: rest.ContentConfig{},
	}

	if bearerTokenFile != "" {
		config.BearerTokenFile = bearerTokenFile
	} else if bearerToken != "" {
		config.BearerToken = bearerToken
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	metricsClient, err := metrics.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &client{
		kubeClient:    kubeClient,
		metricsClient: metricsClient,
		timeout:       timeout,
		namespace:     namespace,
	}, nil
}

func (c *client) getDaemonSets(ctx context.Context) (*appsv1.DaemonSetList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.AppsV1().DaemonSets(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getDeployments(ctx context.Context) (*appsv1.DeploymentList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.AppsV1().Deployments(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getEndpoints(ctx context.Context) (*corev1.EndpointsList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().Endpoints(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getIngress(ctx context.Context) (*netv1.IngressList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.NetworkingV1().Ingresses(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getNodes(ctx context.Context) (*corev1.NodeList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
}

func (c *client) getPersistentVolumes(ctx context.Context) (*corev1.PersistentVolumeList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
}

func (c *client) getPersistentVolumeClaims(ctx context.Context) (*corev1.PersistentVolumeClaimList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().PersistentVolumeClaims(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getPods(ctx context.Context) (*corev1.PodList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getServices(ctx context.Context) (*corev1.ServiceList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.CoreV1().Services(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getStatefulSets(ctx context.Context) (*appsv1.StatefulSetList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.kubeClient.AppsV1().StatefulSets(c.namespace).List(ctx, metav1.ListOptions{})
}

func (c *client) getPodMetrics(ctx context.Context) (*metricsv1beta1.PodMetricsList, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.metricsClient.MetricsV1beta1().PodMetricses("").List(ctx, metav1.ListOptions{})
}
