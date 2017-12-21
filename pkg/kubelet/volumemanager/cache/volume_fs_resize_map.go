package cache

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

type VolumeToResize struct {
	Pod        *v1.Pod
	VolumeName string
}

// VolumeFsResizeMap defines an interface that serves as
// a cache for holding pending online resizing requests.
type VolumeFsResizeMap interface {
	AddVolumeForPod(volumeName string, pod *v1.Pod)
	GetVolumesToResize() []VolumeToResize
	MarkAsFileSystemResized(pod *v1.Pod, volumeName string) error
}

func NewVolumeFsResizeMap(kubeClient clientset.Interface) VolumeFsResizeMap {
	return &volumeFsResizeMap{
		kubeClient: kubeClient,
		pods:       make(map[types.UniquePodName]volumeToResizeSet),
	}
}

// map volume name(volume.Spec.Name()) to VolumeToResize.
type volumeToResizeSet map[string]VolumeToResize

type volumeFsResizeMap struct {
	sync.Mutex
	pods map[types.UniquePodName]volumeToResizeSet
	// kube client for making API calls
	kubeClient clientset.Interface
}

func (resizeMap *volumeFsResizeMap) AddVolumeForPod(volumeName string, pod *v1.Pod) {
	resizeMap.Lock()
	defer resizeMap.Unlock()
	glog.V(5).Infof("Pending fs resizing request for volume %s by pod %s/%s",
		volumeName, pod.Name, pod.Namespace)
	podName := volumehelper.GetUniquePodName(pod)
	volumes, exist := resizeMap.pods[podName]
	if !exist {
		volumes = make(volumeToResizeSet)
		resizeMap.pods[podName] = volumes
	}
	volumes[volumeName] = VolumeToResize{pod, volumeName}
}

func (resizeMap *volumeFsResizeMap) GetVolumesToResize() []VolumeToResize {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	var volumesToResize []VolumeToResize
	for _, volumes := range resizeMap.pods {
		for _, volume := range volumes {
			volumesToResize = append(volumesToResize, volume)
		}
	}
	resizeMap.pods = make(map[types.UniquePodName]volumeToResizeSet)

	return volumesToResize
}

func (resizeMap *volumeFsResizeMap) MarkAsFileSystemResized(pod *v1.Pod, volumeName string) error {
	if pod == nil {
		return nil
	}

	podObj, err := resizeMap.kubeClient.CoreV1().Pods(pod.Namespace).Get(pod.Name, meta_v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get pod %s/%s with %v", pod.Name, pod.Namespace, err)
	}

	annKey := volumehelper.GetVolumeFsResizeAnnotation(volumeName)
	if _, exist := podObj.Annotations[annKey]; !exist {
		glog.V(5).Infof("Skip MarkAsFileSystemResized for pod %s/%s PVC %s: "+
			"pod's annotation already cleared", podObj.Name, podObj.Namespace, volumeName)
		return nil
	}

	delete(podObj.Annotations, annKey)
	_, err = resizeMap.kubeClient.CoreV1().Pods(podObj.Namespace).Update(podObj)
	if err != nil {
		return fmt.Errorf("failed to update pod %s/%s with %v", podObj.Name, podObj.Namespace, err)
	}

	return nil
}
