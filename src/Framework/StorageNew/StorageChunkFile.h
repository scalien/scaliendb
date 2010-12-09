class StorageChunkFile
{
    typedef InList<StorageChunkDataPage> DataPages;

public:
    StorageChunkHeaderPage  headerPage;
    StorageChunkIndexPage   indexPage;
    StorageChunkBloomPage   bloomPage;

    DataPages               dataPages;
};
