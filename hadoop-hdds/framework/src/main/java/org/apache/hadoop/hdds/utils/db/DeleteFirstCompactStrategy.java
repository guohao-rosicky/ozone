package org.apache.hadoop.hdds.utils.db;

import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DeleteFirstCompactStrategy implements RDBCompactStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeleteFirstCompactStrategy.class);

  private RocksDatabase rocksDB;
  private RocksDatabase.ColumnFamily columnFamily;

  private double ratio;

  public DeleteFirstCompactStrategy(RocksDatabase rocksDB,
                                    RocksDatabase.ColumnFamily columnFamily) {
    this.rocksDB = rocksDB;
    this.columnFamily = columnFamily;
  }




  public boolean compactIfNeed() throws IOException {

    List<LiveFileMetaData> liveFileMetaDataList =
        rocksDB.getLiveFilesMetaData();

    for (LiveFileMetaData file : liveFileMetaDataList) {

      if (columnFamily.getName().equals(file.columnFamilyName())) {
        file.numDeletions();
        file.numEntries();

        file.smallestKey();
        file.largestKey();
      }
    }





    return true;
  }







}
