#ifndef STORAGEDEFAULTS_H
#define STORAGEDEAULTS_H


#define DEFAULT_KEY_LIMIT           1000
#define DEFAULT_DATAPAGE_SIZE       (64*1024)
#define DEFAULT_NUM_DATAPAGES       256         // 16.7 MB wort of data pages
#define DEFAULT_INDEXPAGE_SIZE      (256*1024)  // to fit 256 keys

//#define DEFAULT_KEY_LIMIT         1000
//#define DEFAULT_DATAPAGE_SIZE     256*1024
//#define DEFAULT_NUM_DATAPAGES     256         // 16.7 MB wort of data pages
//#define DEFAULT_INDEXPAGE_SIZE        256*1024    // to fit 256 keys

//#define DEFAULT_KEY_LIMIT         100
//#define DEFAULT_DATAPAGE_SIZE     4*1024
//#define DEFAULT_NUM_DATAPAGES     16*256          // 16.7 MB wort of data pages
//#define DEFAULT_INDEXPAGE_SIZE        16+16*256*(100+8)

#define STORAGE_DEFAULT_CACHE_SIZE          (1000UL*DEFAULT_DATAPAGE_SIZE)

#endif
