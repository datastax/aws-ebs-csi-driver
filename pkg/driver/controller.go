/*
Copyright 2019 The Kubernetes Authors.

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

package driver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/awslabs/volume-modifier-for-k8s/pkg/rpc"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/coalescer"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/driver/internal"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/util"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/util/template"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

// Supported access modes.
const (
	SingleNodeWriter     = csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
	MultiNodeMultiWriter = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
)

var (
	// controllerCaps represents the capability of controller service.
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
	}
)

const trueStr = "true"
const isManagedByDriver = trueStr

// ControllerService represents the controller service of CSI driver.
type ControllerService struct {
	cloud                 cloud.Cloud
	inFlight              *internal.InFlight
	options               *Options
	modifyVolumeCoalescer coalescer.Coalescer[modifyVolumeRequest, int32]
	rpc.UnimplementedModifyServer
	csi.UnimplementedControllerServer
}

// NewControllerService creates a new controller service.
func NewControllerService(c cloud.Cloud, o *Options) *ControllerService {
	return &ControllerService{
		cloud:                 c,
		options:               o,
		inFlight:              internal.NewInFlight(),
		modifyVolumeCoalescer: newModifyVolumeCoalescer(c, o),
	}
}

func (d *ControllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).InfoS("CreateVolume: called", "args", util.SanitizeRequest(req))
	if err := validateCreateVolumeRequest(req); err != nil {
		return nil, err
	}
	volSizeBytes, err := getVolSizeBytes(req)
	if err != nil {
		return nil, err
	}
	volName := req.GetName()
	volCap := req.GetVolumeCapabilities()

	multiAttach := false
	for _, c := range volCap {
		if c.GetAccessMode().GetMode() == MultiNodeMultiWriter && isBlock(c) {
			klog.V(4).InfoS("CreateVolume: multi-attach is enabled", "volumeID", volName)
			multiAttach = true
		}
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volName); !ok {
		msg := fmt.Sprintf("Create volume request for %s is already in progress", volName)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volName)

	var (
		volumeType             string
		iopsPerGB              int32
		allowIOPSPerGBIncrease bool
		iops                   int32
		throughput             int32
		isEncrypted            bool
		blockExpress           bool
		kmsKeyID               string
		scTags                 []string
		volumeTags             = map[string]string{
			cloud.VolumeNameTagKey:   volName,
			cloud.AwsEbsDriverTagKey: isManagedByDriver,
		}
		blockSize       string
		inodeSize       string
		bytesPerInode   string
		numberOfInodes  string
		ext4BigAlloc    bool
		ext4ClusterSize string
		raidVolumeCount int32
		raidType        string
	)

	tProps := new(template.PVProps)

	for key, value := range req.GetParameters() {
		switch strings.ToLower(key) {
		case "fstype":
			klog.InfoS("\"fstype\" is deprecated, please use \"csi.storage.k8s.io/fstype\" instead")
		case VolumeTypeKey:
			volumeType = value
		case IopsPerGBKey:
			parseIopsPerGBKey, parseIopsPerGBKeyErr := strconv.ParseInt(value, 10, 32)
			if parseIopsPerGBKeyErr != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse invalid iopsPerGB: %v", err)
			}
			iopsPerGB = int32(parseIopsPerGBKey)
		case AllowAutoIOPSPerGBIncreaseKey:
			allowIOPSPerGBIncrease = isTrue(value)
		case IopsKey:
			parseIopsKey, parseIopsKeyErr := strconv.ParseInt(value, 10, 32)
			if parseIopsKeyErr != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse invalid iops: %v", err)
			}
			iops = int32(parseIopsKey)
		case ThroughputKey:
			parseThroughput, parseThroughputErr := strconv.ParseInt(value, 10, 32)
			if parseThroughputErr != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse invalid throughput: %v", err)
			}
			throughput = int32(parseThroughput)
		case EncryptedKey:
			isEncrypted = isTrue(value)
		case KmsKeyIDKey:
			kmsKeyID = value
		case PVCNameKey:
			volumeTags[PVCNameTag] = value
			tProps.PVCName = value
		case PVCNamespaceKey:
			volumeTags[PVCNamespaceTag] = value
			tProps.PVCNamespace = value
		case PVNameKey:
			volumeTags[PVNameTag] = value
			tProps.PVName = value
		case BlockExpressKey:
			blockExpress = isTrue(value)
		case BlockSizeKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse blockSize (%s): %v", value, err)
			}
			blockSize = value
		case InodeSizeKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse inodeSize (%s): %v", value, err)
			}
			inodeSize = value
		case BytesPerInodeKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse bytesPerInode (%s): %v", value, err)
			}
			bytesPerInode = value
		case NumberOfInodesKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse numberOfInodes (%s): %v", value, err)
			}
			numberOfInodes = value
		case Ext4BigAllocKey:
			ext4BigAlloc = isTrue(value)
		case Ext4ClusterSizeKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse ext4ClusterSize (%s): %v", value, err)
			}
			ext4ClusterSize = value
		case RaidTypeKey:
			if isAlphanumeric := util.StringIsAlphanumeric(value); !isAlphanumeric {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse raidTypeKey (%s): %v", value, err)
			}
			raidType = value
		case RaidStripeCountKey:
			raidVolumeCountKey, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse raidVolumesKey: %v", err)
			}
			raidVolumeCount = int32(raidVolumeCountKey)
		default:
			if strings.HasPrefix(key, TagKeyPrefix) {
				scTags = append(scTags, value)
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid parameter key %s for CreateVolume", key)
			}
		}
	}

	modifyOptions, err := parseModifyVolumeParameters(req.GetMutableParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid mutable parameter: %v", err)
	}

	// "Values specified in mutable_parameters MUST take precedence over the values from parameters."
	// https://github.com/container-storage-interface/spec/blob/master/spec.md#createvolume
	if modifyOptions.modifyDiskOptions.VolumeType != "" {
		volumeType = modifyOptions.modifyDiskOptions.VolumeType
	}
	if modifyOptions.modifyDiskOptions.IOPS != 0 {
		iops = modifyOptions.modifyDiskOptions.IOPS
	}
	if modifyOptions.modifyDiskOptions.Throughput != 0 {
		throughput = modifyOptions.modifyDiskOptions.Throughput
	}

	responseCtx := map[string]string{}

	if len(blockSize) > 0 {
		responseCtx[BlockSizeKey] = blockSize
		if err = validateFormattingOption(volCap, BlockSizeKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}
	if len(inodeSize) > 0 {
		responseCtx[InodeSizeKey] = inodeSize
		if err = validateFormattingOption(volCap, InodeSizeKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}
	if len(bytesPerInode) > 0 {
		responseCtx[BytesPerInodeKey] = bytesPerInode
		if err = validateFormattingOption(volCap, BytesPerInodeKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}
	if len(numberOfInodes) > 0 {
		responseCtx[NumberOfInodesKey] = numberOfInodes
		if err = validateFormattingOption(volCap, NumberOfInodesKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}
	if ext4BigAlloc {
		responseCtx[Ext4BigAllocKey] = trueStr
		if err = validateFormattingOption(volCap, Ext4BigAllocKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}
	if len(ext4ClusterSize) > 0 {
		responseCtx[Ext4ClusterSizeKey] = ext4ClusterSize
		if err = validateFormattingOption(volCap, Ext4ClusterSizeKey, FileSystemConfigs); err != nil {
			return nil, err
		}
	}

	if !ext4BigAlloc && len(ext4ClusterSize) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Cannot set ext4BigAllocClusterSize when ext4BigAlloc is false")
	}

	if blockExpress && volumeType != cloud.VolumeTypeIO2 {
		return nil, status.Errorf(codes.InvalidArgument, "Block Express is only supported on io2 volumes")
	}

	if raidType != "" && raidVolumeCount < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "RaidVolumeCount must be greater than 1")
	}

	if raidType != "" && raidType != RaidType0 {
		// Only RAID 0 is supported for now
		return nil, status.Errorf(codes.InvalidArgument, "RaidType %s is not supported", raidType)
	}

	snapshotID := ""
	volumeSource := req.GetVolumeContentSource()
	if volumeSource != nil {
		if _, ok := volumeSource.GetType().(*csi.VolumeContentSource_Snapshot); !ok {
			return nil, status.Error(codes.InvalidArgument, "Unsupported volumeContentSource type")
		}
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, status.Error(codes.InvalidArgument, "Error retrieving snapshot from the volumeContentSource")
		}
		snapshotID = sourceSnapshot.GetSnapshotId()
	}

	// create a new volume
	zone := pickAvailabilityZone(req.GetAccessibilityRequirements())
	outpostArn := getOutpostArn(req.GetAccessibilityRequirements())

	// fill volume tags
	if d.options.KubernetesClusterID != "" {
		resourceLifecycleTag := ResourceLifecycleTagPrefix + d.options.KubernetesClusterID
		volumeTags[resourceLifecycleTag] = ResourceLifecycleOwned
		volumeTags[NameTag] = d.options.KubernetesClusterID + "-dynamic-" + volName
		volumeTags[KubernetesClusterTag] = d.options.KubernetesClusterID
	}
	for k, v := range d.options.ExtraTags {
		volumeTags[k] = v
	}

	addTags, err := template.Evaluate(scTags, tProps, d.options.WarnOnInvalidTag)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error interpolating the tag value: %v", err)
	}

	if err = validateExtraTags(addTags, d.options.WarnOnInvalidTag); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid tag value: %v", err)
	}

	for k, v := range addTags {
		volumeTags[k] = v
	}

	opts := &cloud.DiskOptions{
		CapacityBytes:          volSizeBytes,
		Tags:                   volumeTags,
		VolumeType:             volumeType,
		IOPSPerGB:              iopsPerGB,
		AllowIOPSPerGBIncrease: allowIOPSPerGBIncrease,
		IOPS:                   iops,
		Throughput:             throughput,
		AvailabilityZone:       zone,
		OutpostArn:             outpostArn,
		Encrypted:              isEncrypted,
		BlockExpress:           blockExpress,
		KmsKeyID:               kmsKeyID,
		SnapshotID:             snapshotID,
		MultiAttachEnabled:     multiAttach,
	}

	if raidType == RaidType0 {
		sizePerDisk := calculateRaid0StripeSize(volSizeBytes, raidVolumeCount)
		disks := make([]*cloud.Disk, 0, raidVolumeCount)
		klog.InfoS("Creating RAID 0 volume", "stripes", raidVolumeCount, "sizePerDisk", sizePerDisk, "volName", volName)

		responseCtx[RaidStripeCountKey] = strconv.Itoa(int(raidVolumeCount))
		responseCtx[RaidTypeKey] = raidType
		responseCtx[PVCVolumeName] = volName
		responseCtx[RaidVolumeSize] = strconv.FormatInt(volSizeBytes, 10)

		for i := int32(0); i < raidVolumeCount; i++ {
			raidOpts := *opts
			raidOpts.CapacityBytes = sizePerDisk
			volumeTags[RaidStripeCountKey] = strconv.Itoa(int(raidVolumeCount))
			volStripeName := fmt.Sprintf("%s-%d", volName, i)

			disk, err := d.cloud.CreateDisk(ctx, volStripeName, &raidOpts)
			if err != nil {
				var errCode codes.Code
				switch {
				case errors.Is(err, cloud.ErrNotFound):
					errCode = codes.NotFound
				case errors.Is(err, cloud.ErrIdempotentParameterMismatch), errors.Is(err, cloud.ErrAlreadyExists):
					errCode = codes.AlreadyExists
				default:
					errCode = codes.Internal
				}
				return nil, status.Errorf(errCode, "Could not create volume %q: %v", volName, err)
			}

			klog.InfoS("Created disk: ", "volumeId", disk.VolumeID, "volumeName", volName, "volumeSizeInGiB", disk.CapacityGiB)

			disks = append(disks, disk)
		}
		return newCreateRaidVolumeResponse(disks, volSizeBytes, responseCtx), nil
	}

	disk, err := d.cloud.CreateDisk(ctx, volName, opts)
	if err != nil {
		var errCode codes.Code
		switch {
		case errors.Is(err, cloud.ErrNotFound):
			errCode = codes.NotFound
		case errors.Is(err, cloud.ErrIdempotentParameterMismatch), errors.Is(err, cloud.ErrAlreadyExists):
			errCode = codes.AlreadyExists
		default:
			errCode = codes.Internal
		}
		return nil, status.Errorf(errCode, "Could not create volume %q: %v", volName, err)
	}
	return newCreateVolumeResponse(disk, responseCtx), nil
}

func calculateRaid0StripeSize(volumeSize int64, raidVolumeCount int32) int64 {
	// RAID 0 stripe size is the volume size divided by the number of volumes
	// We add a single GiB to the size to ensure we have enough space for LVM metadata and then
	// round up the entire thing to next full GiB for each volume
	return int64(math.Ceil(float64(volumeSize+util.GiBToBytes(1)) / float64(raidVolumeCount)))
}

func validateCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	volName := req.GetName()
	if len(volName) == 0 {
		return status.Error(codes.InvalidArgument, "Volume name not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if !isValidVolumeCapabilities(volCaps) {
		return status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}
	return nil
}

func (d *ControllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).InfoS("DeleteVolume: called", "args", util.SanitizeRequest(req))
	if err := validateDeleteVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volumeID); !ok {
		msg := fmt.Sprintf(internal.VolumeOperationAlreadyExistsErrorMsg, volumeID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volumeID)

	// TODO This only works if the PVC naming follows this syntax, fix this.
	if strings.HasPrefix(volumeID, "pvc-") {
		/*
			I0211 16:49:24.006948       1 controller.go:580] "ControllerUnpublishVolume: detaching" volumeID="pvc-77e1afc7-1e98-46d1-80d4-e53a2c2fe54d" nodeID="i-0eab5248cfd0d346b"
			E0211 16:49:24.006991       1 driver.go:108] "GRPC error" err="rpc error: code = Internal desc = Could not detach volume \"pvc-77e1afc7-1e98-46d1-80d4-e53a2c2fe54d\" from node \"i-0eab5248cfd0d346b\": batched DescribeVolumes not supported"
		*/

		/*
			Pod delete:

			I0213 16:22:25.257290       1 controller.go:597] "ControllerUnpublishVolume: detaching" volumeID="pvc-00822178-a99d-40eb-a030-0f5699f1dbff" nodeID="i-08ae874092d518e71"
			I0213 16:22:25.257313       1 controller.go:602] Detected raid volume in ControllerUnpublishVolumevolumeIDpvc-00822178-a99d-40eb-a030-0f5699f1dbff
			I0213 16:22:25.433799       1 controller.go:614] Detaching raid diskpvcNamepvc-00822178-a99d-40eb-a030-0f5699f1dbffvolumeIDvol-02c0c02eedd521a29
			I0213 16:22:27.013044       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-02c0c02eedd521a29" actual="detaching" desired="detached"
			I0213 16:22:28.586499       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-02c0c02eedd521a29" actual="detaching" desired="detached"
			I0213 16:22:31.007658       1 controller.go:614] Detaching raid diskpvcNamepvc-00822178-a99d-40eb-a030-0f5699f1dbffvolumeIDvol-0df35f69b03be992f
			I0213 16:22:32.676736       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-0df35f69b03be992f" actual="detaching" desired="detached"
			I0213 16:22:34.243113       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-0df35f69b03be992f" actual="detaching" desired="detached"
			I0213 16:22:36.652988       1 controller.go:614] Detaching raid diskpvcNamepvc-00822178-a99d-40eb-a030-0f5699f1dbffvolumeIDvol-08c39437c59921fa0
			I0213 16:22:38.175867       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-08c39437c59921fa0" actual="detaching" desired="detached"
			I0213 16:22:39.748139       1 cloud.go:1105] "Waiting for volume state" volumeID="vol-08c39437c59921fa0" actual="detaching" desired="detached"
			I0213 16:22:42.148879       1 controller.go:633] "ControllerUnpublishVolume: detached" volumeID="pvc-00822178-a99d-40eb-a030-0f5699f1dbff" nodeID="i-08ae874092d518e71"
		*/

		// We need to detach all the volumes
		disks, err := d.cloud.GetDisksByName(ctx, volumeID)
		if err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.InfoS("ControllerUnpublishVolume: attachment not found", "volumeID", volumeID)
				return &csi.DeleteVolumeResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not detach volume %q: %v", volumeID, err)
		}

		for _, disk := range disks {
			if _, err := d.cloud.DeleteDisk(ctx, disk.VolumeID); err != nil {
				if errors.Is(err, cloud.ErrNotFound) {
					klog.InfoS("ControllerUnpublishVolume: attachment not found", "volumeID", disk.VolumeID)
					return &csi.DeleteVolumeResponse{}, nil
				}
				return nil, status.Errorf(codes.Internal, "Could not delete volume %q: %v", disk.VolumeID, err)
			}
		}
	} else {
		if _, err := d.cloud.DeleteDisk(ctx, volumeID); err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.V(4).InfoS("DeleteVolume: volume not found, returning with success")
				return &csi.DeleteVolumeResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not delete volume ID %q: %v", volumeID, err)
		}
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func validateDeleteVolumeRequest(req *csi.DeleteVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID not provided")
	}
	return nil
}

func (d *ControllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.InfoS("ControllerPublishVolume: called", "args", util.SanitizeRequest(req))
	if err := validateControllerPublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId()

	if !d.inFlight.Insert(volumeID + nodeID) {
		return nil, status.Error(codes.Aborted, fmt.Sprintf(internal.VolumeOperationAlreadyExistsErrorMsg, volumeID))
	}
	defer d.inFlight.Delete(volumeID + nodeID)

	klog.V(2).InfoS("ControllerPublishVolume: attaching", "volumeID", volumeID, "nodeID", nodeID)

	devicePath := ""
	if _, exists := req.VolumeContext[RaidTypeKey]; exists {
		raidVolumeCount, err := strconv.Atoi(req.VolumeContext[RaidStripeCountKey])
		klog.InfoS("ControllerPublishVolume: raid volume detected", "volumeID", volumeID, "nodeID", nodeID, "raidVolumeCount", raidVolumeCount)
		// We need to attach all the volumes
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Could not parse raidVolumeCount: %v", err)
		}

		devicePaths := make([]string, 0, raidVolumeCount)
		for i := 0; i < raidVolumeCount; i++ {
			diskVolumeId := req.VolumeContext[fmt.Sprintf("%s-%d", RaidVolumeIDPrefix, i)]
			// raidVolumeName := fmt.Sprintf("%s-%d", volumeID, i)
			klog.InfoS("Trying to attach raid disk: ", "volumeId", diskVolumeId, "index", i)
			/*
								I0211 16:52:07.504606       1 controller.go:382] "Created disk: " volumeId="vol-014b7901c98de3a46" volumeName="pvc-7ec6f83c-7611-460a-92b1-c84b7a80d511" volumeSizeInGiB=1
								I0211 16:52:09.552962       1 controller.go:382] "Created disk: " volumeId="vol-086d2d26decd17e93" volumeName="pvc-7ec6f83c-7611-460a-92b1-c84b7a80d511" volumeSizeInGiB=1
								I0211 16:52:11.619035       1 controller.go:382] "Created disk: " volumeId="vol-074e0f625fabb1255" volumeName="pvc-7ec6f83c-7611-460a-92b1-c84b7a80d511" volumeSizeInGiB=1
				I0211 16:52:12.379400       1 controller.go:513] "Trying to attach raid disk: " volumeId="vol-014b7901c98de3a46" index=0
				I0211 16:52:14.048310       1 controller.go:522] "Attached raid disk: " volumeId="vol-014b7901c98de3a46" index=0 devicePath="/dev/xvdab"
				I0211 16:52:14.048338       1 controller.go:513] "Trying to attach raid disk: " volumeId="vol-086d2d26decd17e93" index=1
				I0211 16:52:15.687662       1 controller.go:522] "Attached raid disk: " volumeId="vol-086d2d26decd17e93" index=1 devicePath="/dev/xvdac"
				I0211 16:52:15.687687       1 controller.go:513] "Trying to attach raid disk: " volumeId="vol-074e0f625fabb1255" index=2
				I0211 16:52:17.326307       1 controller.go:522] "Attached raid disk: " volumeId="vol-074e0f625fabb1255" index=2 devicePath="/dev/xvdad"
			*/

			if devicePathValue, err := d.cloud.AttachDisk(ctx, diskVolumeId, nodeID); err != nil {
				if errors.Is(err, cloud.ErrNotFound) {
					klog.InfoS("ControllerPublishVolume: volume not found", "volumeID", diskVolumeId, "nodeID", nodeID)
					return nil, status.Errorf(codes.NotFound, "Volume %q not found", diskVolumeId)
				}
				return nil, status.Errorf(codes.Internal, "Could not attach volume %q to node %q: %v", diskVolumeId, nodeID, err)
			} else {
				klog.InfoS("Attached raid disk: ", "volumeId", diskVolumeId, "index", i, "devicePath", devicePathValue)
				devicePaths = append(devicePaths, devicePathValue)
			}
		}
		devicePath = strings.Join(devicePaths, ",")
	} else {
		devicePathValue, err := d.cloud.AttachDisk(ctx, volumeID, nodeID)
		if err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.InfoS("ControllerPublishVolume: volume not found", "volumeID", volumeID, "nodeID", nodeID)
				return nil, status.Errorf(codes.NotFound, "Volume %q not found", volumeID)
			}
			return nil, status.Errorf(codes.Internal, "Could not attach volume %q to node %q: %v", volumeID, nodeID, err)
		}
		devicePath = devicePathValue
	}

	klog.InfoS("ControllerPublishVolume: attached", "volumeID", volumeID, "nodeID", nodeID, "devicePath", devicePath)

	pvInfo := map[string]string{DevicePathKey: devicePath, RaidStripeCountKey: req.VolumeContext[RaidStripeCountKey], RaidTypeKey: req.VolumeContext[RaidTypeKey]}
	return &csi.ControllerPublishVolumeResponse{PublishContext: pvInfo}, nil
}

func validateControllerPublishVolumeRequest(req *csi.ControllerPublishVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	if len(req.GetNodeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !isValidCapability(volCap) {
		return status.Error(codes.InvalidArgument, "Volume capability not supported")
	}
	return nil
}

func (d *ControllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(4).InfoS("ControllerUnpublishVolume: called", "args", util.SanitizeRequest(req))

	if err := validateControllerUnpublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId()

	if !d.inFlight.Insert(volumeID + nodeID) {
		return nil, status.Error(codes.Aborted, fmt.Sprintf(internal.VolumeOperationAlreadyExistsErrorMsg, volumeID))
	}
	defer d.inFlight.Delete(volumeID + nodeID)

	klog.V(2).InfoS("ControllerUnpublishVolume: detaching", "volumeID", volumeID, "nodeID", nodeID)

	// TODO This only works if the PVC naming follows this syntax, fix this.
	// We could fetch by tags also, or check the existing VG group if we have this name..
	if strings.HasPrefix(volumeID, "pvc-") {
		klog.InfoS("Detected raid volume in ControllerUnpublishVolume", "volumeID", volumeID)
		// We need to detach all the volumes
		disks, err := d.cloud.GetDisksByName(ctx, volumeID)
		if err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.InfoS("ControllerUnpublishVolume: attachment not found", "volumeID", volumeID, "nodeID", nodeID)
				return &csi.ControllerUnpublishVolumeResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not get disks for volume %q: %v", volumeID, err)
		}

		for _, disk := range disks {
			klog.InfoS("Detaching raid disk", "pvcName", volumeID, "volumeID", disk.VolumeID)
			if err := d.cloud.DetachDisk(ctx, disk.VolumeID, nodeID); err != nil {
				if errors.Is(err, cloud.ErrNotFound) {
					klog.InfoS("ControllerUnpublishVolume: attachment not found", "volumeID", disk.VolumeID, "nodeID", nodeID)
					return &csi.ControllerUnpublishVolumeResponse{}, nil
				}
				return nil, status.Errorf(codes.Internal, "Could not detach volume %q from node %q: %v", disk.VolumeID, nodeID, err)
			}
		}
	} else {
		if err := d.cloud.DetachDisk(ctx, volumeID, nodeID); err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.InfoS("ControllerUnpublishVolume: attachment not found", "volumeID", volumeID, "nodeID", nodeID)
				return &csi.ControllerUnpublishVolumeResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not detach volume %q from node %q: %v", volumeID, nodeID, err)
		}
	}

	klog.InfoS("ControllerUnpublishVolume: detached", "volumeID", volumeID, "nodeID", nodeID)

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func validateControllerUnpublishVolumeRequest(req *csi.ControllerUnpublishVolumeRequest) error {
	if len(req.GetVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	if len(req.GetNodeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	return nil
}

func (d *ControllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).InfoS("ControllerGetCapabilities: called", "args", req)

	caps := make([]*csi.ControllerServiceCapability, 0, len(controllerCaps))
	for _, capability := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: capability,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *ControllerService) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).InfoS("GetCapacity: called", "args", req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *ControllerService) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).InfoS("ListVolumes: called", "args", req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *ControllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).InfoS("ValidateVolumeCapabilities: called", "args", req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if _, err := d.cloud.GetDiskByID(ctx, volumeID); err != nil {
		if errors.Is(err, cloud.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "Volume not found")
		}
		return nil, status.Errorf(codes.Internal, "Could not get volume with ID %q: %v", volumeID, err)
	}

	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if isValidVolumeCapabilities(volCaps) {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (d *ControllerService) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).InfoS("ControllerExpandVolume: called", "args", util.SanitizeRequest(req))
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// TODO If using raid, reject this.

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}

	newSize := util.RoundUpBytes(capRange.GetRequiredBytes())
	maxVolSize := capRange.GetLimitBytes()
	if maxVolSize > 0 && maxVolSize < newSize {
		return nil, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
	}

	actualSizeGiB, err := d.modifyVolumeCoalescer.Coalesce(volumeID, modifyVolumeRequest{
		newSize: newSize,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q: %v", volumeID, err)
	}

	nodeExpansionRequired := true
	// if this is a raw block device, no expansion should be necessary on the node
	capability := req.GetVolumeCapability()
	if capability != nil && capability.GetBlock() != nil {
		nodeExpansionRequired = false
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         util.GiBToBytes(actualSizeGiB),
		NodeExpansionRequired: nodeExpansionRequired,
	}, nil
}

func (d *ControllerService) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	klog.V(4).InfoS("ControllerModifyVolume: called", "args", util.SanitizeRequest(req))

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	options, err := parseModifyVolumeParameters(req.GetMutableParameters())
	if err != nil {
		return nil, err
	}

	_, err = d.modifyVolumeCoalescer.Coalesce(volumeID, modifyVolumeRequest{
		modifyDiskOptions: options.modifyDiskOptions,
		modifyTagsOptions: options.modifyTagsOptions,
	})
	if err != nil {
		return nil, err
	}

	return &csi.ControllerModifyVolumeResponse{}, nil
}

func (d *ControllerService) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	klog.V(4).InfoS("ControllerGetVolume: called", "args", req)
	return nil, status.Error(codes.Unimplemented, "")
}

func isValidVolumeCapabilities(v []*csi.VolumeCapability) bool {
	for _, c := range v {
		if !isValidCapability(c) {
			return false
		}
	}
	return true
}

func isValidCapability(c *csi.VolumeCapability) bool {
	accessMode := c.GetAccessMode().GetMode()

	//nolint:exhaustive
	switch accessMode {
	case SingleNodeWriter:
		return true

	case MultiNodeMultiWriter:
		if isBlock(c) {
			return true
		} else {
			klog.InfoS("isValidCapability: access mode is only supported for block devices", "accessMode", accessMode)
			return false
		}

	default:
		klog.InfoS("isValidCapability: access mode is not supported", "accessMode", accessMode)
		return false
	}
}

func isBlock(capability *csi.VolumeCapability) bool {
	_, isBlk := capability.GetAccessType().(*csi.VolumeCapability_Block)
	return isBlk
}

func isValidVolumeContext(volContext map[string]string) bool {
	// There could be multiple volume attributes in the volumeContext map
	// Validate here case by case
	if partition, ok := volContext[VolumeAttributePartition]; ok {
		partitionInt, err := strconv.ParseInt(partition, 10, 64)
		if err != nil {
			klog.ErrorS(err, "failed to parse partition as int", "partition", partition)
			return false
		}
		if partitionInt < 0 {
			klog.ErrorS(err, "invalid partition config", "partition", partition)
			return false
		}
	}
	return true
}

func (d *ControllerService) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).InfoS("CreateSnapshot: called", "args", util.SanitizeRequest(req))
	if err := validateCreateSnapshotRequest(req); err != nil {
		return nil, err
	}

	snapshotName := req.GetName()
	volumeID := req.GetSourceVolumeId()
	var outpostArn string

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(snapshotName); !ok {
		msg := fmt.Sprintf(internal.VolumeOperationAlreadyExistsErrorMsg, snapshotName)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(snapshotName)

	snapshot, err := d.cloud.GetSnapshotByName(ctx, snapshotName)
	if err != nil && !errors.Is(err, cloud.ErrNotFound) {
		klog.ErrorS(err, "Error looking for the snapshot", "snapshotName", snapshotName)
		return nil, err
	}
	if snapshot != nil {
		if snapshot.SourceVolumeID != volumeID {
			return nil, status.Errorf(codes.AlreadyExists, "Snapshot %s already exists for different volume (%s)", snapshotName, snapshot.SourceVolumeID)
		}
		klog.V(4).InfoS("Snapshot of volume already exists; nothing to do", "snapshotName", snapshotName, "volumeId", volumeID)
		return newCreateSnapshotResponse(snapshot), nil
	}

	snapshotTags := map[string]string{
		cloud.SnapshotNameTagKey: snapshotName,
		cloud.AwsEbsDriverTagKey: isManagedByDriver,
	}

	var vscTags []string
	var fsrAvailabilityZones []string
	vsProps := new(template.VolumeSnapshotProps)
	for key, value := range req.GetParameters() {
		switch strings.ToLower(key) {
		case VolumeSnapshotNameKey:
			vsProps.VolumeSnapshotName = value
		case VolumeSnapshotNamespaceKey:
			vsProps.VolumeSnapshotNamespace = value
		case VolumeSnapshotContentNameKey:
			vsProps.VolumeSnapshotContentName = value
		case FastSnapshotRestoreAvailabilityZones:
			f := strings.ReplaceAll(value, " ", "")
			fsrAvailabilityZones = strings.Split(f, ",")
		case OutpostArnKey:
			if arn.IsARN(value) {
				outpostArn = value
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid parameter value %s is not a valid arn", value)
			}
		default:
			if strings.HasPrefix(key, TagKeyPrefix) {
				vscTags = append(vscTags, value)
			} else {
				return nil, status.Errorf(codes.InvalidArgument, "Invalid parameter key %s for CreateSnapshot", key)
			}
		}
	}

	addTags, err := template.Evaluate(vscTags, vsProps, d.options.WarnOnInvalidTag)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error interpolating the tag value: %v", err)
	}

	if err = validateExtraTags(addTags, d.options.WarnOnInvalidTag); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid tag value: %v", err)
	}

	if d.options.KubernetesClusterID != "" {
		resourceLifecycleTag := ResourceLifecycleTagPrefix + d.options.KubernetesClusterID
		snapshotTags[resourceLifecycleTag] = ResourceLifecycleOwned
		snapshotTags[NameTag] = d.options.KubernetesClusterID + "-dynamic-" + snapshotName
	}
	for k, v := range d.options.ExtraTags {
		snapshotTags[k] = v
	}

	for k, v := range addTags {
		snapshotTags[k] = v
	}

	opts := &cloud.SnapshotOptions{
		Tags:       snapshotTags,
		OutpostArn: outpostArn,
	}

	// Check if the availability zone is supported for fast snapshot restore
	if len(fsrAvailabilityZones) > 0 {
		zones, err := d.cloud.AvailabilityZones(ctx)
		if err != nil {
			klog.ErrorS(err, "failed to get availability zones")
		} else {
			klog.V(4).InfoS("Availability Zones", "zone", zones)
			for _, az := range fsrAvailabilityZones {
				if _, ok := zones[az]; !ok {
					return nil, status.Errorf(codes.InvalidArgument, "Availability zone %s is not supported for fast snapshot restore", az)
				}
			}
		}
	}

	snapshot, err = d.cloud.CreateSnapshot(ctx, volumeID, opts)
	if err != nil {
		if errors.Is(err, cloud.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Snapshot %q already exists", snapshotName)
		}
		return nil, status.Errorf(codes.Internal, "Could not create snapshot %q: %v", snapshotName, err)
	}

	if len(fsrAvailabilityZones) > 0 {
		_, err := d.cloud.EnableFastSnapshotRestores(ctx, fsrAvailabilityZones, snapshot.SnapshotID)
		if err != nil {
			if _, deleteErr := d.cloud.DeleteSnapshot(ctx, snapshot.SnapshotID); deleteErr != nil {
				return nil, status.Errorf(codes.Internal, "Could not delete snapshot ID %q: %v", snapshotName, deleteErr)
			}
			return nil, status.Errorf(codes.Internal, "Failed to create Fast Snapshot Restores for snapshot ID %q: %v", snapshotName, err)
		}
	}
	return newCreateSnapshotResponse(snapshot), nil
}

func validateCreateSnapshotRequest(req *csi.CreateSnapshotRequest) error {
	if len(req.GetName()) == 0 {
		return status.Error(codes.InvalidArgument, "Snapshot name not provided")
	}

	if len(req.GetSourceVolumeId()) == 0 {
		return status.Error(codes.InvalidArgument, "Snapshot volume source ID not provided")
	}
	return nil
}

func (d *ControllerService) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).InfoS("DeleteSnapshot: called", "args", util.SanitizeRequest(req))
	if err := validateDeleteSnapshotRequest(req); err != nil {
		return nil, err
	}

	snapshotID := req.GetSnapshotId()

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(snapshotID); !ok {
		msg := fmt.Sprintf("DeleteSnapshot for Snapshot %s is already in progress", snapshotID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(snapshotID)

	if _, err := d.cloud.DeleteSnapshot(ctx, snapshotID); err != nil {
		if errors.Is(err, cloud.ErrNotFound) {
			klog.V(4).InfoS("DeleteSnapshot: snapshot not found, returning with success")
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not delete snapshot ID %q: %v", snapshotID, err)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func validateDeleteSnapshotRequest(req *csi.DeleteSnapshotRequest) error {
	if len(req.GetSnapshotId()) == 0 {
		return status.Error(codes.InvalidArgument, "Snapshot ID not provided")
	}
	return nil
}

func (d *ControllerService) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).InfoS("ListSnapshots: called", "args", util.SanitizeRequest(req))
	var snapshots []*cloud.Snapshot

	snapshotID := req.GetSnapshotId()
	if len(snapshotID) != 0 {
		snapshot, err := d.cloud.GetSnapshotByID(ctx, snapshotID)
		if err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				klog.V(4).InfoS("ListSnapshots: snapshot not found, returning with success")
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not get snapshot ID %q: %v", snapshotID, err)
		}
		snapshots = append(snapshots, snapshot)
		response := newListSnapshotsResponse(&cloud.ListSnapshotsResponse{
			Snapshots: snapshots,
		})
		return response, nil
	}

	volumeID := req.GetSourceVolumeId()
	nextToken := req.GetStartingToken()
	maxEntries := req.GetMaxEntries()

	cloudSnapshots, err := d.cloud.ListSnapshots(ctx, volumeID, maxEntries, nextToken)
	if err != nil {
		if errors.Is(err, cloud.ErrNotFound) {
			klog.V(4).InfoS("ListSnapshots: snapshot not found, returning with success")
			return &csi.ListSnapshotsResponse{}, nil
		}
		if errors.Is(err, cloud.ErrInvalidMaxResults) {
			return nil, status.Errorf(codes.InvalidArgument, "Error mapping MaxEntries to AWS MaxResults: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "Could not list snapshots: %v", err)
	}

	response := newListSnapshotsResponse(cloudSnapshots)
	return response, nil
}

// pickAvailabilityZone selects 1 zone given topology requirement.
// if not found, empty string is returned.
func pickAvailabilityZone(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		zone, exists := topology.GetSegments()[WellKnownZoneTopologyKey]
		if exists {
			return zone
		}

		zone, exists = topology.GetSegments()[ZoneTopologyKey]
		if exists {
			return zone
		}
	}
	for _, topology := range requirement.GetRequisite() {
		zone, exists := topology.GetSegments()[WellKnownZoneTopologyKey]
		if exists {
			return zone
		}
		zone, exists = topology.GetSegments()[ZoneTopologyKey]
		if exists {
			return zone
		}
	}
	return ""
}

func getOutpostArn(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		_, exists := topology.GetSegments()[AwsOutpostIDKey]
		if exists {
			return BuildOutpostArn(topology.GetSegments())
		}
	}
	for _, topology := range requirement.GetRequisite() {
		_, exists := topology.GetSegments()[AwsOutpostIDKey]
		if exists {
			return BuildOutpostArn(topology.GetSegments())
		}
	}

	return ""
}

func newCreateRaidVolumeResponse(disks []*cloud.Disk, totalSize int64, ctx map[string]string) *csi.CreateVolumeResponse {
	segments := map[string]string{WellKnownZoneTopologyKey: disks[0].AvailabilityZone}

	arn, err := arn.Parse(disks[0].OutpostArn)

	if err == nil {
		segments[AwsRegionKey] = arn.Region
		segments[AwsPartitionKey] = arn.Partition
		segments[AwsAccountIDKey] = arn.AccountID
		segments[AwsOutpostIDKey] = strings.ReplaceAll(arn.Resource, "outpost/", "")
	}

	pvcName := ctx[PVCVolumeName]
	for i, disk := range disks {
		ctx[fmt.Sprintf("%s-%d", RaidVolumeIDPrefix, i)] = disk.VolumeID
	}

	res := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      pvcName, // This is intentionally not a real volumeID
			CapacityBytes: totalSize,
			VolumeContext: ctx,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: segments,
				},
			},
		},
	}

	return res
}

func newCreateVolumeResponse(disk *cloud.Disk, ctx map[string]string) *csi.CreateVolumeResponse {
	var src *csi.VolumeContentSource
	if disk.SnapshotID != "" {
		src = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: disk.SnapshotID,
				},
			},
		}
	}

	segments := map[string]string{WellKnownZoneTopologyKey: disk.AvailabilityZone}

	arn, err := arn.Parse(disk.OutpostArn)

	if err == nil {
		segments[AwsRegionKey] = arn.Region
		segments[AwsPartitionKey] = arn.Partition
		segments[AwsAccountIDKey] = arn.AccountID
		segments[AwsOutpostIDKey] = strings.ReplaceAll(arn.Resource, "outpost/", "")
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      disk.VolumeID,
			CapacityBytes: util.GiBToBytes(disk.CapacityGiB),
			VolumeContext: ctx,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: segments,
				},
			},
			ContentSource: src,
		},
	}
}

func newCreateSnapshotResponse(snapshot *cloud.Snapshot) *csi.CreateSnapshotResponse {
	ts := timestamppb.New(snapshot.CreationTime)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshot.SnapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			SizeBytes:      util.GiBToBytes(snapshot.Size),
			CreationTime:   ts,
			ReadyToUse:     snapshot.ReadyToUse,
		},
	}
}

func newListSnapshotsResponse(cloudResponse *cloud.ListSnapshotsResponse) *csi.ListSnapshotsResponse {
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(cloudResponse.Snapshots))
	for _, snapshot := range cloudResponse.Snapshots {
		snapshotResponseEntry := newListSnapshotsResponseEntry(snapshot)
		entries = append(entries, snapshotResponseEntry)
	}
	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: cloudResponse.NextToken,
	}
}

func newListSnapshotsResponseEntry(snapshot *cloud.Snapshot) *csi.ListSnapshotsResponse_Entry {
	ts := timestamppb.New(snapshot.CreationTime)

	return &csi.ListSnapshotsResponse_Entry{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshot.SnapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			SizeBytes:      util.GiBToBytes(snapshot.Size),
			CreationTime:   ts,
			ReadyToUse:     snapshot.ReadyToUse,
		},
	}
}

func getVolSizeBytes(req *csi.CreateVolumeRequest) (int64, error) {
	var volSizeBytes int64
	capRange := req.GetCapacityRange()
	if capRange == nil {
		volSizeBytes = cloud.DefaultVolumeSize
	} else {
		volSizeBytes = util.RoundUpBytes(capRange.GetRequiredBytes())
		maxVolSize := capRange.GetLimitBytes()
		if maxVolSize > 0 && maxVolSize < volSizeBytes {
			return 0, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
		}
	}
	return volSizeBytes, nil
}

// BuildOutpostArn returns the string representation of the outpost ARN from the given csi.TopologyRequirement.segments.
func BuildOutpostArn(segments map[string]string) string {
	if len(segments[AwsPartitionKey]) == 0 {
		return ""
	}

	if len(segments[AwsRegionKey]) == 0 {
		return ""
	}
	if len(segments[AwsOutpostIDKey]) == 0 {
		return ""
	}
	if len(segments[AwsAccountIDKey]) == 0 {
		return ""
	}

	return fmt.Sprintf("arn:%s:outposts:%s:%s:outpost/%s",
		segments[AwsPartitionKey],
		segments[AwsRegionKey],
		segments[AwsAccountIDKey],
		segments[AwsOutpostIDKey],
	)
}

func validateFormattingOption(volumeCapabilities []*csi.VolumeCapability, paramName string, fsConfigs map[string]fileSystemConfig) error {
	for _, volCap := range volumeCapabilities {
		if isBlock(volCap) {
			return status.Error(codes.InvalidArgument, fmt.Sprintf("Cannot use %s with block volume", paramName))
		}

		mountVolume := volCap.GetMount()
		if mountVolume == nil {
			return status.Error(codes.InvalidArgument, "CreateVolume: mount is nil within volume capability")
		}

		fsType := mountVolume.GetFsType()
		if supported := fsConfigs[fsType].isParameterSupported(paramName); !supported {
			return status.Errorf(codes.InvalidArgument, "Cannot use %s with fstype %s", paramName, fsType)
		}
	}

	return nil
}

func isTrue(value string) bool {
	return value == trueStr
}
