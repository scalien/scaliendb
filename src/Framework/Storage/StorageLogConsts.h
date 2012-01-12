
#define STORAGE_LOGSECTION_HEAD_SIZE            (8+8+4)
                                                // (8) size
                                                // (8) uncomressedLength
                                                // (4) CRC

#define STORAGE_LOGSEGMENT_COMMAND_SET          's'
#define STORAGE_LOGSEGMENT_COMMAND_DELETE       'd'

#define STORAGE_LOGSEGMENT_WRITE_GRANULARITY    (64*KiB)
#define STORAGE_LOGSEGMENT_VERSION              (1)

#define STORAGE_LOGSEGMENT_FILENAME_FORMAT      "log.%020U.%020U"
