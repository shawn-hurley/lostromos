package bundlectlr

import (
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/automationbroker/bundle-lib/apb"
	"github.com/automationbroker/bundle-lib/runtime"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/wpengine/lostromos/metrics"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	restclient "k8s.io/client-go/rest"
)

type genericBundleResource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              apb.Parameters         `json:"spec"`
	Status            map[string]interface{} `json:"status"`
}

func (g genericBundleResource) toUnstructured() *unstructured.Unstructured {
	m := map[string]interface{}{}

	b, _ := json.Marshal(g)
	json.Unmarshal(b, m)

	return &unstructured.Unstructured{Object: m}

}

const (
	// ImagePullPolicy - pull policy
	ImagePullPolicy = "Always"
	// defaultNS - default
	defaultNS         = "default"
	serviceInstanceID = "serviceInstanceID"
	parameterHashKey  = "parameterHash"
	apbPlanKey        = "_apb_plan_id"
)

// Controller - controller for Bundles
type Controller struct {
	Spec        *apb.Spec // image name for the bundle
	Namespace   string    // Namespace that the controller should deploy the bundle to.
	SandboxRole string    // Sandbox role to be used. Will default to edit
	planName    string
	logger      *zap.SugaredLogger
	dc          dynamic.ResourceInterface
}

// NewController - create the new controller
func NewController(ns, sr, spec64Yaml, group, version, pn, plan string, logger *zap.SugaredLogger, kubeCfg *restclient.Config) *Controller {
	if ns == "" {
		ns = defaultNS
	}
	// Get the dynamic Client
	kubeCfg.ContentConfig.GroupVersion = &schema.GroupVersion{
		Group:   group,
		Version: version,
	}
	kubeCfg.APIPath = "apis"
	dynClient, err := dynamic.NewClient(kubeCfg)
	apiResource := &metav1.APIResource{
		Name:       pn,
		Namespaced: true,
	}
	decodedSpecYaml, err := b64.StdEncoding.DecodeString(spec64Yaml)
	if err != nil {
		fmt.Printf("err - %v", err)
		return nil
	}
	spec := &apb.Spec{}

	if err = yaml.Unmarshal(decodedSpecYaml, spec); err != nil {
		fmt.Printf("err - %v", err)
		return nil
	}

	// Set up the cluster config.
	cc := apb.ClusterConfig{
		PullPolicy:    "always",
		SandboxRole:   sr,
		Namespace:     ns,
		KeepNamespace: true,
	}
	apb.InitializeClusterConfig(cc)
	runtime.NewRuntime(runtime.Configuration{})
	logrus.SetLevel(logrus.DebugLevel)
	c := &Controller{
		Namespace:   ns,
		SandboxRole: sr,
		Spec:        spec,
		planName:    plan,
		logger:      logger,
		dc:          dynClient.Resource(apiResource, ns),
	}
	_, err = c.dc.List(metav1.ListOptions{})
	if err != nil {
		c.logger.Infow("unable to list bundles", "err", err.Error())
	}
	spec.Image = "docker.io/ansibleplaybookbundle/postgresql-apb:latest"
	spec.ID = "123123123"
	spec.Runtime = 2
	logger.Infow("completed spec parse", "spec", spec)
	return c
}

// ResourceAdded - handle when the resource is added
func (c Controller) ResourceAdded(r *unstructured.Unstructured) {
	metrics.TotalEvents.Inc()
	c.logger.Debugw("resource added", "unstructred.Unstructured:", r)

	s := genericBundleResource{Status: map[string]interface{}{}}
	b, err := r.MarshalJSON()
	if err != nil {
		c.logger.Errorw("resource added", "unstructred.Unstructured:", err)
	}
	json.Unmarshal(b, &s)
	//set hash to check for parameter changes.
	h := sha1.New()
	b, err = json.Marshal(s.Spec)
	if err != nil {
		c.logger.Errorw("resource added", "unstructred.Unstructured:", err)
	}
	h.Write(b)
	s.Status[parameterHashKey] = fmt.Sprintf("%x", h.Sum(nil))

	// Generate a new UUID for now for the ServiceInstance
	id := uuid.NewRandom()
	// status
	s.Status[serviceInstanceID] = id
	c.updateStatus(s)
	s.Spec[parameterHashKey] = c.planName
	si := apb.ServiceInstance{
		ID:   id,
		Spec: c.Spec,
		Context: &apb.Context{
			Platform:     "kubernetes",
			Namespace:    c.Namespace,
			NotSandboxed: true,
		},
		Parameters: &s.Spec,
	}
	c.logger.Infow("using service instance", "service instance", si)
	ex := apb.NewExecutor()
	channel := ex.Provision(&si)
	messages := []apb.StatusMessage{}
	s.Status["messages"] = messages
	for status := range channel {
		messages = append(messages, status)
		s.Status["messages"] = messages
		c.updateStatus(s)
		c.logger.Infow("messages from channel", "message", status)
	}
}

func (c Controller) updateStatus(s genericBundleResource) {
	c.logger.Debugw("update status", "Generic Bundle Resource", s)
	rnew, err := c.dc.Get(s.GetName(), metav1.GetOptions{})
	if err != nil {
		c.logger.Errorw("unable to update status", "Generic Bundle Resource", s, "err", err.Error())
	}
	rnew.Object["status"] = s.Status
	_, err = c.dc.Update(rnew)
	if err != nil {
		c.logger.Errorw("unable to update status", "Generic Bundle Resouce", s, "err", err.Error())
	}
}

// ResourceDeleted - handle when the resource is deleted
func (c Controller) ResourceDeleted(r *unstructured.Unstructured) {
	c.logger.Infow("resource deleted", "unstructred.Unstructured:", r)
}

// ResourceUpdated - handle when the resource is updated
func (c Controller) ResourceUpdated(oldR, newR *unstructured.Unstructured) {
	newGBR := genericBundleResource{Status: map[string]interface{}{}}

	b, err := newR.MarshalJSON()
	if err != nil {
		c.logger.Errorw("resource added", "unstructred.Unstructured:", err)
	}
	json.Unmarshal(b, &newGBR)
	// Hash the parameters of new. Compare to the old parameters.
	h := sha1.New()
	b, err = json.Marshal(newGBR.Spec)
	if err != nil {
		c.logger.Errorw("resource added", "unstructred.Unstructured:", err)
	}
	h.Write(b)
	newParams := fmt.Sprintf("%x", h.Sum(nil))
	p, ok := newGBR.Status[parameterHashKey]
	if !ok {
		c.logger.Infow("resource not updated unable to find parameter hash.", "unstructred.Unstructured:", oldR, "unstructred.Unstructured:", newR)
		return
	}
	oldParams, ok := p.(string)
	if newParams == oldParams || !ok {
		c.logger.Infow("resource not updated", "unstructred.Unstructured:", oldR, "unstructred.Unstructured:", newR)
		return
	}
	c.logger.Infow("resource updated", "unstructred.Unstructured:", oldR, "unstructred.Unstructured:", newR)
	newGBR.Status[parameterHashKey] = newParams
	c.updateStatus(newGBR)
	id := uuid.Parse((newGBR.Status[serviceInstanceID]).(string))
	newGBR.Spec[parameterHashKey] = c.planName
	si := apb.ServiceInstance{
		ID:   id,
		Spec: c.Spec,
		Context: &apb.Context{
			Platform:     "kubernetes",
			Namespace:    c.Namespace,
			NotSandboxed: true,
		},
		Parameters: &newGBR.Spec,
	}
	c.logger.Infow("using service instance", "service instance", si)
	ex := apb.NewExecutor()
	channel := ex.Update(&si)
	messages := []apb.StatusMessage{}
	newGBR.Status["messages"] = messages
	for status := range channel {
		messages = append(messages, status)
		newGBR.Status["messages"] = messages
		c.updateStatus(newGBR)
		c.logger.Infow("messages from channel", "message", status)
	}
}
