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

package rbd

import (
	"fmt"
	"os"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	volutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

// NewDeviceMounter initializes a DeviceMounter.
func (plugin *rbdPlugin) NewDeviceMounter() (volume.DeviceMounter, error) {
	return plugin.newDeviceMounterInternal(&RBDUtil{})
}

func (plugin *rbdPlugin) newDeviceMounterInternal(manager diskManager) (volume.DeviceMounter, error) {
	return &rbdDeviceMounter{
		plugin:  plugin,
		manager: manager,
		mounter: volumehelper.NewSafeFormatAndMountFromHost(plugin.GetPluginName(), plugin.host),
	}, nil
}

// NewDeviceUmounter initializes a DeviceUmounter.
func (plugin *rbdPlugin) NewDeviceUmounter() (volume.DeviceUmounter, error) {
	return plugin.newDeviceUmounterInternal(&RBDUtil{})
}

func (plugin *rbdPlugin) newDeviceUmounterInternal(manager diskManager) (volume.DeviceUmounter, error) {
	return &rbdDeviceUmounter{
		plugin:  plugin,
		manager: manager,
		mounter: volumehelper.NewSafeFormatAndMountFromHost(plugin.GetPluginName(), plugin.host),
	}, nil
}

// GetDeviceMountRefs implements AttachableVolumePlugin.GetDeviceMountRefs.
func (plugin *rbdPlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter(plugin.GetPluginName())
	return mount.GetMountRefs(mounter, deviceMountPath)
}

// rbdDeviceMounter implements volume.DeviceMounter interface.
type rbdDeviceMounter struct {
	plugin  *rbdPlugin
	mounter *mount.SafeFormatAndMount
	manager diskManager
}

var _ volume.DeviceMounter = &rbdDeviceMounter{}

// GetDeviceMountPath implements Attacher.GetDeviceMountPath.
func (deviceMounter *rbdDeviceMounter) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	img, err := getVolumeSourceImage(spec)
	if err != nil {
		return "", err
	}
	pool, err := getVolumeSourcePool(spec)
	if err != nil {
		return "", err
	}
	return makePDNameInternal(deviceMounter.plugin.host, pool, img), nil
}

// MountDevice implements DeviceMounter.MountDevice. It is called by the kubelet to
// mount device at the given mount path.
// This method is idempotent, callers are responsible for retrying on failure.
func (deviceMounter *rbdDeviceMounter) MountDevice(spec *volume.Spec, _ string, pod *v1.Pod) (string, error) {
	mounter, err := deviceMounter.plugin.createMounterFromVolumeSpecAndPod(spec, pod)
	if err != nil {
		glog.Warningf("failed to create mounter: %v", spec)
		return "", err
	}
	realDevicePath, err := deviceMounter.manager.AttachDisk(*mounter)
	if err != nil {
		return "", err
	}
	glog.V(3).Infof("rbd: successfully attach volume (spec: %s, pool: %s, image: %s) at %s", spec.Name(), mounter.Pool, mounter.Image, realDevicePath)

	deviceMountPath, err := deviceMounter.GetDeviceMountPath(spec)
	if err != nil {
		return deviceMountPath, err
	}

	glog.V(4).Infof("rbd: mouting device %s to %s", realDevicePath, deviceMountPath)
	notMnt, err := deviceMounter.mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return deviceMountPath, err
			}
			notMnt = true
		} else {
			return deviceMountPath, err
		}
	}
	if !notMnt {
		return deviceMountPath, nil
	}
	fstype, err := getVolumeSourceFSType(spec)
	if err != nil {
		return deviceMountPath, err
	}
	ro, err := getVolumeSourceReadOnly(spec)
	if err != nil {
		return deviceMountPath, err
	}
	options := []string{}
	if ro {
		options = append(options, "ro")
	}
	mountOptions := volume.MountOptionFromSpec(spec, options...)
	err = deviceMounter.mounter.FormatAndMount(realDevicePath, deviceMountPath, fstype, mountOptions)
	if err != nil {
		os.Remove(deviceMountPath)
		return deviceMountPath, fmt.Errorf("rbd: failed to mount device %s at %s (fstype: %s), error %v", realDevicePath, deviceMountPath, fstype, err)
	}
	glog.V(3).Infof("rbd: successfully mount device %s at %s (fstype: %s)", realDevicePath, deviceMountPath, fstype)
	return deviceMountPath, nil
}

// rbdDeviceUmounter implements volume.DeviceUmounter interface.
type rbdDeviceUmounter struct {
	plugin  *rbdPlugin
	manager diskManager
	mounter *mount.SafeFormatAndMount
}

var _ volume.DeviceUmounter = &rbdDeviceUmounter{}

// UnmountDevice implements Detacher.UnmountDevice. It unmounts the global
// mount of the RBD image. This is called once all bind mounts have been
// unmounted.
// Internally, it does four things:
//  - Unmount device from deviceMountPath.
//  - Detach device from the node.
//  - Remove lock if found. (No need to check volume readonly or not, because
//  device is not on the node anymore, it's safe to remove lock.)
//  - Remove the deviceMountPath at last.
// This method is idempotent, callers are responsible for retrying on failure.
func (detacher *rbdDeviceUmounter) UnmountDevice(deviceMountPath string) error {
	if pathExists, pathErr := volutil.PathExists(deviceMountPath); pathErr != nil {
		return fmt.Errorf("Error checking if path exists: %v", pathErr)
	} else if !pathExists {
		glog.Warningf("Warning: Unmount skipped because path does not exist: %v", deviceMountPath)
		return nil
	}
	devicePath, cnt, err := mount.GetDeviceNameFromMount(detacher.mounter, deviceMountPath)
	if err != nil {
		return err
	}
	if cnt > 1 {
		return fmt.Errorf("rbd: more than 1 reference counts at %s", deviceMountPath)
	}
	if cnt == 1 {
		// Unmount the device from the device mount point.
		glog.V(4).Infof("rbd: unmouting device mountpoint %s", deviceMountPath)
		if err = detacher.mounter.Unmount(deviceMountPath); err != nil {
			return err
		}
		glog.V(3).Infof("rbd: successfully umount device mountpath %s", deviceMountPath)
	}
	glog.V(4).Infof("rbd: detaching device %s", devicePath)
	err = detacher.manager.DetachDisk(detacher.plugin, deviceMountPath, devicePath)
	if err != nil {
		return err
	}
	glog.V(3).Infof("rbd: successfully detach device %s", devicePath)
	err = os.Remove(deviceMountPath)
	if err != nil {
		return err
	}
	glog.V(3).Infof("rbd: successfully remove device mount point %s", deviceMountPath)
	return nil
}
