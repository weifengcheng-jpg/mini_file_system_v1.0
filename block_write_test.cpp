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
	
	
	//2. 写入文件到主块文件中
	std::stringstream tmp_stream;
	tmp_stream<< "." << largefile::MAINBLOCK_DIR_PREFIX << block_id;
	tmp_stream >> mainblock_path;
	
	largefile::FileOperation *mainblock = new largefile::FileOperation(mainblock_path, O_RDWR | O_LARGEFILE | O_CREAT);
	
	char buffer[4096];
	memset(buffer, '6', sizeof(buffer));
	
	int32_t data_offset = index_handle->get_block_data_offset();
	uint32_t file_no = index_handle->block_info()->seq_no_;
	
	if( (ret = mainblock->pwrite_file(buffer, sizeof(buffer), data_offset)) !=  largefile::TFS_SUCCESS)
	{
		fprintf(stderr, "write to main block failed. ret: %d, reason: %s\n", ret, strerror(errno));
		mainblock->close_file();
		
		delete mainblock;
		delete index_handle;
		exit(-3);
	}
	
	//3. 索引文件中写入Metainfo
	largefile::MetaInfo meta;
	meta.set_file_id( file_no );
	meta.set_offset( data_offset );
	meta.set_size( sizeof(buffer) );
	
	ret = index_handle->write_segment_meta(meta.get_key(), meta);
	if(ret == largefile::TFS_SUCCESS)
	{
		//1.更新索引头部信息
		index_handle->commit_block_data_offset( sizeof(buffer) );
		//2. 更新块信息
		index_handle->update_block_info(largefile::C_OPER_INSERT, sizeof(buffer) );
		
		ret = index_handle->flush();
		
		if(ret != largefile::TFS_SUCCESS)
		{
			fprintf(stderr, "flush mainblock %d failed. file no: %u \n", block_id, file_no);
			
		}
	}else 
	{
		fprintf(stderr, "write_segement_meta - mainblock %d failed. file no: %u\n", block_id, file_no);
	}
	
	if(ret !=  largefile::TFS_SUCCESS){
		fprintf(stderr,"write to mainblock %d  failed. file no: %u\n", block_id, file_no);
	}else 
	{
		if(debug) printf("write successfully. file_no : %u , block_id: %d\n", file_no, block_id);
	}
	
	mainblock->close_file();
	
	delete mainblock;
	delete index_handle;
	
	return 0;
}