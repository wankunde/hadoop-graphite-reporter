package com.wankun.hadoop.reporter.collector;

import static com.wankun.hadoop.reporter.util.SinkUtil.name;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.MetricRegistry;

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
  public MetricRegistry collect(JSONArray beans) {
    MetricRegistry metrics = new MetricRegistry();
    for (int i = 0; i < beans.size(); i++) {
      JSONObject bean = beans.getJSONObject(i);
      String name = bean.getString("name");
      String host = bean.getString("tag.Hostname");
      if (name.startsWith("Hadoop:service=NameNode,name=FSNamesystem")) {
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
        if ("active".equals(bean.getString("tag.HAState"))) {
          for (String key : keys) {
            metrics.gauge(name("FSNamesystem", host, key), () -> () -> bean.get(key));
          }
        }

      } else if (name.startsWith("Hadoop:service=NameNode,name=NameNodeInfo")) {
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
          metrics.gauge(name("NameNodeInfo", host, key), () -> () -> bean.get(key));
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
          metrics.gauge(name("RpcActivity", host, port, key), () -> () -> bean.get(key));
        }
      } else if (name.startsWith("Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020")) {
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
          metrics.gauge(name("RpcDetailedActivityForPort8020", host, key), () -> () -> bean.get(key));
        }
      } else if (name.startsWith("Hadoop:service=NameNode,name=RpcDetailedActivityForPort8022")) {
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
          metrics.gauge(name("RpcDetailedActivityForPort8022", host, key), () -> () -> bean.get(key));
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
          metrics.gauge(name("NameNodeActivity", host, key), () -> () -> bean.get(key));
        }
      } else if (name.startsWith("Hadoop:service=NameNode,name=FSNamesystemState")) {
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
          metrics.gauge(name("FSNamesystemState", host, key), () -> () -> bean.get(key));
        }
      }
    }
    return metrics;
  }
}
