package populator

import (
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

// VolumeFsResizeMapPopulator periodically loops through the list of active pods
// and ensures that each one exists in the volume fs resize map if it has one or
// more volumeFSResizingAnnotation.
// VolumeFsResizeMapPopulator doesn't verify that the pods in the volume fs resize
// map still exist as the DesiredStateOfWorldPopulator will do this verification,
// Resize request will be skipped if according pods not exist in the actual state
// of world.
type VolumeFsResizeMapPopulator interface {
	Run(stopCh <-chan struct{})
}

func NewVolumeFsResizeMapPopulator(
	loopSleepDuration time.Duration,
	podManager pod.Manager,
	podStatusProvider status.PodStatusProvider,
	volumeFsResizeMap cache.VolumeFsResizeMap) VolumeFsResizeMapPopulator {
	return &volumeFsResizeMapPopulator{
		loopSleepDuration: loopSleepDuration,
		podManager:        podManager,
		podStatusProvider: podStatusProvider,
		volumeFsResizeMap: volumeFsResizeMap,
	}
}

type volumeFsResizeMapPopulator struct {
	loopSleepDuration      time.Duration
	podManager             pod.Manager
	podStatusProvider      status.PodStatusProvider
	timeOfLastGetPodStatus time.Time
	volumeFsResizeMap      cache.VolumeFsResizeMap
}

func (populator *volumeFsResizeMapPopulator) Run(stopCh <-chan struct{}) {
	wait.Until(populator.populate, populator.loopSleepDuration, stopCh)
}

func (populator *volumeFsResizeMapPopulator) populate() {
	populator.addNewPods()
}

func (populator *volumeFsResizeMapPopulator) addNewPods() {
	for _, podObj := range populator.podManager.GetPods() {
		if populator.isPodTerminated(podObj) {
			// Do not resize volumes for terminated pods.
			continue
		}
		populator.processPodVolumes(podObj)
	}
}

// TODO: Extract this method as common func as it is also used by DesiredStateOfWorldPopulator.
func (populator *volumeFsResizeMapPopulator) isPodTerminated(pod *v1.Pod) bool {
	podStatus, found := populator.podStatusProvider.GetPodStatus(pod.UID)
	if !found {
		podStatus = pod.Status
	}
	return volumehelper.IsPodTerminated(pod, podStatus)
}

func (populator *volumeFsResizeMapPopulator) processPodVolumes(pod *v1.Pod) {
	if pod == nil {
		return
	}
	for key, value := range pod.Annotations {
		if volumehelper.IsVolumeFsResizeAnnotation(key) && value == "yes" {
			populator.volumeFsResizeMap.AddVolumeForPod(
				volumehelper.GetVolumeUniqueNameFromFsResizeAnnotation(key), pod.DeepCopy())
		}
	}
}
