#include "common.h"
#include "file_op.h"
#include "index_handle.h"
#include <sstream>

using namespace qiniu;
using namespace std;

const static largefile::MMapOption mmap_option = {1024000, 4096, 4096};  //内存映射的参数
const static uint32_t main_blocksize = 1024*1024*64;  //主块文件的大小 
const static uint32_t bucket_size = 1000;  //哈希桶的大小
static int32_t block_id = 1;

static int debug = 1;

int main(int argc, char **argv)   //argv[0]="rm"  argv[1]="-f"  argv[2] ="a.out"
{
	std::string mainblock_path;
	std::string index_path;
	int32_t ret = largefile::TFS_SUCCESS;
	
	
	cout<<" Type your bockid :"<<endl;
	cin>> block_id;
	
	if(block_id < 1){
		cerr<<" Invalid blockid, exit."<<endl;
		exit(-1);
	}
	
	
	
	//1. 加载索引文件
	largefile::IndexHandle *index_handle = new largefile::IndexHandle(".", block_id);  //索引文件句柄
	
	if(debug)  printf("load index ...\n");
	
	ret = index_handle->load(block_id, bucket_size, mmap_option);
	
	if(ret != largefile::TFS_SUCCESS)
	{
		fprintf(stderr, "load index %d failed.\n", block_id);
		//delete mainblock;
		delete index_handle;
		exit(-2);
	}
	
	//2.  读取文件的meta info.
	uint64_t file_id = 0;
	cout<<" Type your file_id :"<<endl;
	cin>> file_id;
	
	if(file_id < 1){
		cerr<<" Invalid fileid, exit."<<endl;
		exit(-2);
	}
	
	largefile::MetaInfo meta;
	
	ret = index_handle->read_segment_meta( file_id, meta);
	if(ret != largefile::TFS_SUCCESS)
	{
		fprintf(stderr, "read_segment_meta error. file_id: %lu, ret: %d\n", file_id, ret);
		exit(-3);
	}
	
	//3. 根据meta info 读取文件
	std::stringstream tmp_stream;
	tmp_stream<< "." << largefile::MAINBLOCK_DIR_PREFIX << block_id;
	tmp_stream >> mainblock_path;
	
	largefile::FileOperation *mainblock = new largefile::FileOperation(mainblock_path, O_RDWR );
	char *buffer = new char[meta.get_size() + 1];
	
	ret = mainblock->pread_file(buffer, meta.get_size(), meta.get_offset());
	if(ret != largefile::TFS_SUCCESS)
	{
		fprintf(stderr, "read from main block failed. ret: %d, reason: %s\n", ret, strerror(errno));
		mainblock->close_file();
		
		delete mainblock;
		delete index_handle;
		exit(-3);
	}
	
	buffer[meta.get_size()] = '\0';
	printf("read size %d, content: %s\n", meta.get_size(), buffer);
	
	mainblock->close_file();
	delete mainblock;
	delete index_handle;
	return 0;
}