/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	commontypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/controller/volume/expand/util"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/types"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

// PodsInVolumeGetter defines a method to fetch all pods using a volume with specified name.
type PodsInVolumeGetter interface {
	// Get all pods using the volume with specified name
	GetPodsInVolume(volumeName v1.UniqueVolumeName) []*v1.Pod
}

type VolumeMountedNodesGetter interface {
	GetMountedNodesForVolume(volumeName v1.UniqueVolumeName) []commontypes.NodeName
}

// VolumeResizeMap defines an interface that serves as a cache for holding pending resizing requests
type VolumeResizeMap interface {
	// AddPVCUpdate adds pvc for resizing
	AddPVCUpdate(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume)
	// DeletePVC deletes pvc that is scheduled for resizing
	DeletePVC(pvc *v1.PersistentVolumeClaim)
	// GetPVCsWithResizeRequest returns all pending pvc resize requests
	GetPVCsWithResizeRequest() []*PVCWithResizeRequest
	// MarkAsResized marks a pvc as fully resized
	MarkAsResized(*PVCWithResizeRequest, resource.Quantity) error
	// UpdatePVSize updates just pv size after cloudprovider resizing is successful
	UpdatePVSize(*PVCWithResizeRequest, resource.Quantity) error
	// MarkForFileSystemResize mark the PVC need resizing by kubelet.
	MarkForFileSystemResize(*volume.Spec, v1.UniqueVolumeName) error
}

type volumeResizeMap struct {
	// map of unique pvc name and resize requests that are pending or inflight
	pvcrs map[types.UniquePVCName]*PVCWithResizeRequest
	// kube client for making API calls
	kubeClient clientset.Interface
	// for guarding access to pvcrs map
	sync.RWMutex
	// Used to get pod info with specified namespace and name.
	podLister corelisters.PodLister
	// Used to fetch all pods using a volume
	podsInVolumeGetter PodsInVolumeGetter
	// Used to fetch all nodes mounted a volume.
	volumeMountedNodesGetter VolumeMountedNodesGetter
}

// PVCWithResizeRequest struct defines data structure that stores state needed for
// performing file system resize
type PVCWithResizeRequest struct {
	// PVC that needs to be resized
	PVC *v1.PersistentVolumeClaim
	// persistentvolume
	PersistentVolume *v1.PersistentVolume
	// Current volume size
	CurrentSize resource.Quantity
	// Expended volume size
	ExpectedSize resource.Quantity
}

// UniquePVCKey returns unique key of the PVC based on its UID
func (pvcr *PVCWithResizeRequest) UniquePVCKey() types.UniquePVCName {
	return types.UniquePVCName(pvcr.PVC.UID)
}

// QualifiedName returns namespace and name combination of the PVC
func (pvcr *PVCWithResizeRequest) QualifiedName() string {
	return strings.JoinQualifiedName(pvcr.PVC.Namespace, pvcr.PVC.Name)
}

// NewVolumeResizeMap returns new VolumeResizeMap which acts as a cache
// for holding pending resize requests.
func NewVolumeResizeMap(
	kubeClient clientset.Interface,
	podLister corelisters.PodLister,
	podsInVolumeGetter PodsInVolumeGetter,
	volumeMountedNodesGetter VolumeMountedNodesGetter) VolumeResizeMap {
	resizeMap := &volumeResizeMap{}
	resizeMap.pvcrs = make(map[types.UniquePVCName]*PVCWithResizeRequest)
	resizeMap.kubeClient = kubeClient
	resizeMap.podLister = podLister
	resizeMap.podsInVolumeGetter = podsInVolumeGetter
	resizeMap.volumeMountedNodesGetter = volumeMountedNodesGetter
	return resizeMap
}

// AddPVCUpdate adds pvc for resizing
// This function intentionally allows addition of PVCs for which pv.Spec.Size >= pvc.Spec.Size,
// the reason being - lack of transaction in k8s means after successful resize, we can't guarantee that when we update PV,
// pvc update will be successful too and after resize we alyways update PV first.
// If for some reason we weren't able to update PVC after successful resize, then we are going to reprocess
// the PVC and hopefully after a no-op resize in volume plugin, PVC will be updated with right values as well.
func (resizeMap *volumeResizeMap) AddPVCUpdate(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) {
	if pv.Spec.ClaimRef == nil || pvc.Namespace != pv.Spec.ClaimRef.Namespace || pvc.Name != pv.Spec.ClaimRef.Name {
		glog.V(4).Infof("Persistent Volume is not bound to PVC being updated : %s", util.ClaimToClaimKey(pvc))
		return
	}

	if pvc.Status.Phase != v1.ClaimBound {
		return
	}

	resizeMap.Lock()
	defer resizeMap.Unlock()

	pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcStatusSize := pvc.Status.Capacity[v1.ResourceStorage]

	if pvcStatusSize.Cmp(pvcSize) >= 0 {
		return
	}

	glog.V(4).Infof("Adding pvc %s with Size %s/%s for resizing", util.ClaimToClaimKey(pvc), pvcSize.String(), pvcStatusSize.String())

	pvcRequest := &PVCWithResizeRequest{
		PVC:              pvc,
		CurrentSize:      pvcStatusSize,
		ExpectedSize:     pvcSize,
		PersistentVolume: pv,
	}
	resizeMap.pvcrs[types.UniquePVCName(pvc.UID)] = pvcRequest
}

// GetPVCsWithResizeRequest returns all pending pvc resize requests
func (resizeMap *volumeResizeMap) GetPVCsWithResizeRequest() []*PVCWithResizeRequest {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	pvcrs := []*PVCWithResizeRequest{}
	for _, pvcr := range resizeMap.pvcrs {
		pvcrs = append(pvcrs, pvcr)
	}
	// Empty out pvcrs map, we will add back failed resize requests later
	resizeMap.pvcrs = map[types.UniquePVCName]*PVCWithResizeRequest{}
	return pvcrs
}

// DeletePVC removes given pvc object from list of pvcs that needs resizing.
// deleting a pvc in this map doesn't affect operations that are already inflight.
func (resizeMap *volumeResizeMap) DeletePVC(pvc *v1.PersistentVolumeClaim) {
	resizeMap.Lock()
	defer resizeMap.Unlock()
	pvcUniqueName := types.UniquePVCName(pvc.UID)
	glog.V(5).Infof("Removing PVC %v from resize map", pvcUniqueName)
	delete(resizeMap.pvcrs, pvcUniqueName)
}

// MarkAsResized marks a pvc as fully resized
func (resizeMap *volumeResizeMap) MarkAsResized(pvcr *PVCWithResizeRequest, newSize resource.Quantity) error {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	emptyCondition := []v1.PersistentVolumeClaimCondition{}

	err := resizeMap.updatePVCCapacityAndConditions(pvcr, newSize, emptyCondition)
	if err != nil {
		glog.V(4).Infof("Error updating PV spec capacity for volume %q with : %v", pvcr.QualifiedName(), err)
		return err
	}
	return nil
}

// UpdatePVSize updates just pv size after cloudprovider resizing is successful
func (resizeMap *volumeResizeMap) UpdatePVSize(pvcr *PVCWithResizeRequest, newSize resource.Quantity) error {
	resizeMap.Lock()
	defer resizeMap.Unlock()

	oldPv := pvcr.PersistentVolume
	pvClone := oldPv.DeepCopy()

	oldData, err := json.Marshal(pvClone)

	if err != nil {
		return fmt.Errorf("Unexpected error marshaling PV : %q with error %v", pvClone.Name, err)
	}

	pvClone.Spec.Capacity[v1.ResourceStorage] = newSize

	newData, err := json.Marshal(pvClone)

	if err != nil {
		return fmt.Errorf("Unexpected error marshaling PV : %q with error %v", pvClone.Name, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, pvClone)

	if err != nil {
		return fmt.Errorf("Error Creating two way merge patch for  PV : %q with error %v", pvClone.Name, err)
	}

	_, updateErr := resizeMap.kubeClient.CoreV1().PersistentVolumes().Patch(pvClone.Name, commontypes.StrategicMergePatchType, patchBytes)

	if updateErr != nil {
		glog.V(4).Infof("Error updating pv %q with error : %v", pvClone.Name, updateErr)
		return updateErr
	}
	return nil
}

func (resizeMap *volumeResizeMap) updatePVCCapacityAndConditions(pvcr *PVCWithResizeRequest, newSize resource.Quantity, pvcConditions []v1.PersistentVolumeClaimCondition) error {

	claimClone := pvcr.PVC.DeepCopy()

	claimClone.Status.Capacity[v1.ResourceStorage] = newSize
	claimClone.Status.Conditions = pvcConditions

	_, updateErr := resizeMap.kubeClient.CoreV1().PersistentVolumeClaims(claimClone.Namespace).UpdateStatus(claimClone)
	if updateErr != nil {
		glog.V(4).Infof("updating PersistentVolumeClaim[%s] status: failed: %v", pvcr.QualifiedName(), updateErr)
		return updateErr
	}
	return nil
}

// MarkForFileSystemResize add an annotation to pods using the volume with specified name. If one or more pods
// in a node has been updated to reflect this resize operation, skip updating other pods in the same node.
func (resizeMap *volumeResizeMap) MarkForFileSystemResize(
	volumeSpec *volume.Spec,
	uniqueVolumeName v1.UniqueVolumeName) error {
	// Fetch pods using this volume from DesiredStateOfWorld.
	pods := resizeMap.podsInVolumeGetter.GetPodsInVolume(uniqueVolumeName)
	if len(pods) == 0 {
		glog.V(5).Infof("Skip MarkForFileSystemResize %s as no pods using it", uniqueVolumeName)
		return nil
	}

	// Fetch nodes mounted this volume from ActualStateOfWorld.
	mountedNodes := make(map[string]bool)
	for _, nodeName := range resizeMap.volumeMountedNodesGetter.GetMountedNodesForVolume(uniqueVolumeName) {
		mountedNodes[string(nodeName)] = true
	}

	podObjs := make([]*v1.Pod, 0, len(pods))
	markedNodes := make(map[commontypes.NodeName]bool)
	for _, pod := range pods {
		podObj, err := resizeMap.podLister.Pods(pod.Namespace).Get(pod.Name)
		if err != nil {
			return fmt.Errorf("get pod %s/%s using PVC %s failed: %v",
				pod.Name, pod.Namespace, uniqueVolumeName, err)
		}
		podObj = podObj.DeepCopy()

		if len(podObj.Spec.NodeName) == 0 {
			glog.V(5).Infof("Skip MarkForFileSystemResize for pvc %s pod %s/%s: pod not scheduled",
				uniqueVolumeName, pod.Name, pod.Namespace)
			continue
		}

		// If volume hasn't been mounted to the node, we do not add annotation to pods running on this node.
		if !mountedNodes[podObj.Spec.NodeName] {
			glog.V(5).Infof("Skip MarkForFileSystemResize for pvc %s pod %s/%s: "+
				"volume not mounted to %s yet", uniqueVolumeName, pod.Name, pod.Namespace, podObj.Spec.NodeName)
			continue
		}

		// Check whether one or more pods has been updated in the
		// node or not, if so, skip other pods running on same node.
		if markedNodes[commontypes.NodeName(podObj.Spec.NodeName)] {
			continue
		}

		if resizeMap.podHasMarkedForPVCResizing(podObj, volumeSpec.Name()) {
			markedNodes[commontypes.NodeName(podObj.Spec.NodeName)] = true
			glog.V(5).Infof("Skip update pod %s/%s annotation for "+
				"pvc %s resizing as it already updated", podObj.Name, podObj.Namespace, uniqueVolumeName)
		} else {
			podObjs = append(podObjs, podObj)
		}
	}

	for _, podObj := range podObjs {
		if markedNodes[commontypes.NodeName(podObj.Spec.NodeName)] {
			glog.V(5).Infof("Skip MarkForFileSystemResize for pvc %s pod %s/%s: "+
				"another pod in the same node has been marked", uniqueVolumeName, podObj.Name, podObj.Namespace)
			continue
		}

		markErr := resizeMap.updatePodAnnotationForPVCResizing(podObj, volumeSpec.Name())
		if markErr != nil {
			glog.Errorf("Update pod %s/%s annotation for pvc %s resizing failed: %v", podObj.Name, podObj.Namespace, uniqueVolumeName, markErr)
			return markErr
		}
		markedNodes[commontypes.NodeName(podObj.Spec.NodeName)] = true
	}

	return nil
}

func (resizeMap *volumeResizeMap) podHasMarkedForPVCResizing(pod *v1.Pod, pvcName string) bool {
	resizeAnnValue, exist := pod.Annotations[volumehelper.GetVolumeFsResizeAnnotation(pvcName)]
	return exist && resizeAnnValue == "yes"
}

func (resizeMap *volumeResizeMap) updatePodAnnotationForPVCResizing(pod *v1.Pod, pvcName string) error {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[volumehelper.GetVolumeFsResizeAnnotation(pvcName)] = "yes"
	_, err := resizeMap.kubeClient.CoreV1().Pods(pod.Namespace).Update(pod)
	return err
}
