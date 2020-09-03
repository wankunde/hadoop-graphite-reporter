package com.wankun.hadoop.reporter.collector;

import static com.wankun.hadoop.reporter.util.SinkUtil.name;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wankun.hadoop.reporter.Metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-01.
 */
public class NameNodeCollector extends JmxCollector {
  @Override
  public String getName() {
    return "NAMENODE";
  }

  @Override
  public List<Metric> collect(JSONArray beans) {
    List<Metric> metrics = new ArrayList<>();
    for (int i = 0; i < beans.size(); i++) {
      JSONObject bean = beans.getJSONObject(i);
      String name = bean.getString("name");
      // a trick method to filter standby node
      if ("Hadoop:service=NameNode,name=FSNamesystem".equals(name)
          && "active".equals(bean.getInteger("tag.HAState"))) {
        return Collections.EMPTY_LIST;
      }

      String host = bean.getString("tag.Hostname");
      if (name.equals("Hadoop:service=NameNode,name=FSNamesystem")) {
        String[] keys = {
            "BlocksTotal",
            "MissingBlocks",
            "MissingReplOneBlocks",
            "ExpiredHeartbeats",
            "TransactionsSinceLastCheckpoint",
            "TransactionsSinceLastLogRoll",
            "LastWrittenTransactionId",
            "LastCheckpointTime",
            "UnderReplicatedBlocks",
            "CorruptBlocks",
            "CapacityTotal",
            "CapacityTotalGB",
            "CapacityUsed",
            "CapacityUsedGB",
            "CapacityRemaining",
            "CapacityRemainingGB",
            "CapacityUsedNonDFS",
            "TotalLoad",
            "SnapshottableDirectories",
            "Snapshots",
            "NumEncryptionZones",
            "LockQueueLength",
            "FilesTotal",
            "PendingReplicationBlocks",
            "ScheduledReplicationBlocks",
            "PendingDeletionBlocks",
            "ExcessBlocks",
            "PostponedMisreplicatedBlocks",
            "PendingDataNodeMessageCount",
            "MillisSinceLastLoadedEdits",
            "BlockCapacity",
            "StaleDataNodes",
            "TotalFiles"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("FSNamesystem", key), (Number) bean.get(key)));
        }

      } else if (name.equals("Hadoop:service=NameNode,name=NameNodeInfo")) {
        String[] keys = {
            "TotalBlocks",
            "Used",
            "Free",
            "NonDfsUsedSpace",
            "PercentUsed",
            "BlockPoolUsedSpace",
            "PercentBlockPoolUsed",
            "PercentRemaining",
            "CacheCapacity",
            "CacheUsed",
            "TotalFiles",
            "NumberOfMissingBlocks",
            "NumberOfMissingBlocksWithReplicationFactorOne"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("NameNodeInfo", key), (Number) bean.get(key)));
        }
      } else if (name.startsWith("Hadoop:service=NameNode,name=RpcActivityFor")) {
        String port = bean.getString("tag.port");

        String[] keys = {
            "ReceivedBytes",
            "SentBytes",
            "RpcQueueTimeNumOps",
            "RpcQueueTimeAvgTime",
            "RpcProcessingTimeNumOps",
            "RpcProcessingTimeAvgTime",
            "RpcAuthenticationFailures",
            "RpcAuthenticationSuccesses",
            "RpcAuthorizationFailures",
            "RpcAuthorizationSuccesses",
            "RpcSlowCalls",
            "RpcClientBackoff",
            "NumOpenConnections",
            "CallQueueLength",
            "NumDroppedConnections"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("RpcActivity", host, port, key), (Number) bean.get(key)));
        }
      } else if (name.equals("Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020")) {
        String[] keys = {
            "GetBlockLocationsNumOps",
            "GetBlockLocationsAvgTime",
            "FileAlreadyExistsExceptionNumOps",
            "FileAlreadyExistsExceptionAvgTime",
            "ListCachePoolsNumOps",
            "ListCachePoolsAvgTime",
            "FileNotFoundExceptionNumOps",
            "FileNotFoundExceptionAvgTime",
            "DeleteNumOps",
            "DeleteAvgTime",
            "GetServerDefaultsNumOps",
            "GetServerDefaultsAvgTime",
            "SetOwnerNumOps",
            "SetOwnerAvgTime",
            "FsyncNumOps",
            "FsyncAvgTime",
            "GetAdditionalDatanodeNumOps",
            "GetAdditionalDatanodeAvgTime",
            "LeaseExpiredExceptionNumOps",
            "LeaseExpiredExceptionAvgTime",
            "AddBlockNumOps",
            "AddBlockAvgTime",
            "ListEncryptionZonesNumOps",
            "ListEncryptionZonesAvgTime",
            "CreateNumOps",
            "CreateAvgTime",
            "SetPermissionNumOps",
            "SetPermissionAvgTime",
            "GetEZForPathNumOps",
            "GetEZForPathAvgTime",
            "AccessControlExceptionNumOps",
            "AccessControlExceptionAvgTime",
            "SetSafeModeNumOps",
            "SetSafeModeAvgTime",
            "GetListingNumOps",
            "GetListingAvgTime",
            "SetReplicationNumOps",
            "SetReplicationAvgTime",
            "Rename2NumOps",
            "Rename2AvgTime",
            "ListCacheDirectivesNumOps",
            "ListCacheDirectivesAvgTime",
            "RenewLeaseNumOps",
            "RenewLeaseAvgTime",
            "RenameNumOps",
            "RenameAvgTime",
            "MkdirsNumOps",
            "MkdirsAvgTime",
            "SafeModeExceptionNumOps",
            "SafeModeExceptionAvgTime",
            "SetTimesNumOps",
            "SetTimesAvgTime",
            "CompleteNumOps",
            "CompleteAvgTime",
            "GetFileInfoNumOps",
            "GetFileInfoAvgTime"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("RpcDetailedActivityForPort8020", host, key), (Number) bean.get(key)));
        }
      } else if (name.equals("Hadoop:service=NameNode,name=RpcDetailedActivityForPort8022")) {
        String[] keys = {
            "RollEditLogNumOps",
            "RollEditLogAvgTime",
            "GetTransactionIdNumOps",
            "GetTransactionIdAvgTime",
            "CommitBlockSynchronizationNumOps",
            "CommitBlockSynchronizationAvgTime",
            "BlockReportNumOps",
            "BlockReportAvgTime",
            "RegisterDatanodeNumOps",
            "RegisterDatanodeAvgTime",
            "SendHeartbeatNumOps",
            "SendHeartbeatAvgTime",
            "GetEditLogManifestNumOps",
            "GetEditLogManifestAvgTime",
            "CacheReportNumOps",
            "CacheReportAvgTime",
            "BlockReceivedAndDeletedNumOps",
            "BlockReceivedAndDeletedAvgTime",
            "RetriableExceptionNumOps",
            "RetriableExceptionAvgTime",
            "VersionRequestNumOps",
            "VersionRequestAvgTime"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("RpcDetailedActivityForPort8022", host, key), (Number) bean.get(key)));
        }
      } else if (name.startsWith("Hadoop:service=NameNode,name=NameNodeActivity")) {
        String[] keys = {
            "CreateFileOps",
            "FilesCreated",
            "FilesAppended",
            "GetBlockLocations",
            "FilesRenamed",
            "GetListingOps",
            "DeleteFileOps",
            "FilesDeleted",
            "FileInfoOps",
            "AddBlockOps",
            "GetAdditionalDatanodeOps",
            "CreateSymlinkOps",
            "GetLinkTargetOps",
            "FilesInGetListingOps",
            "AllowSnapshotOps",
            "DisallowSnapshotOps",
            "CreateSnapshotOps",
            "DeleteSnapshotOps",
            "RenameSnapshotOps",
            "ListSnapshottableDirOps",
            "SnapshotDiffReportOps",
            "BlockReceivedAndDeletedOps",
            "StorageBlockReportOps",
            "BlockOpsQueued",
            "BlockOpsBatched",
            "TransactionsNumOps",
            "TransactionsAvgTime",
            "SyncsNumOps",
            "SyncsAvgTime",
            "TransactionsBatchedInSync",
            "BlockReportNumOps",
            "BlockReportAvgTime",
            "CacheReportNumOps",
            "CacheReportAvgTime",
            "SafeModeTime",
            "FsImageLoadTime",
            "GetEditNumOps",
            "GetEditAvgTime",
            "GetImageNumOps",
            "GetImageAvgTime",
            "PutImageNumOps",
            "PutImageAvgTime",
            "TotalFileOps"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("NameNodeActivity", host, key), (Number) bean.get(key)));
        }
      } else if (name.equals("Hadoop:service=NameNode,name=FSNamesystemState")) {
        String[] keys = {
            "BlocksTotal",
            "UnderReplicatedBlocks",
            "CapacityTotal",
            "CapacityUsed",
            "CapacityRemaining",
            "TotalLoad",
            "NumEncryptionZones",
            "FsLockQueueLength",
            "MaxObjects",
            "FilesTotal",
            "PendingReplicationBlocks",
            "ScheduledReplicationBlocks",
            "PendingDeletionBlocks",
            "BlockDeletionStartTime",
            "NumLiveDataNodes",
            "NumDeadDataNodes",
            "NumDecomLiveDataNodes",
            "NumDecomDeadDataNodes",
            "VolumeFailuresTotal",
            "EstimatedCapacityLostTotal",
            "NumDecommissioningDataNodes",
            "NumStaleDataNodes",
            "NumStaleStorages",
            "NumInMaintenanceLiveDataNodes",
            "NumInMaintenanceDeadDataNodes",
            "NumEnteringMaintenanceDataNodes"
        };
        for (String key : keys) {
          metrics.add(new Metric(name("FSNamesystemState", host, key), (Number) bean.get(key)));
        }
      }
    }
    return metrics;
  }
}
