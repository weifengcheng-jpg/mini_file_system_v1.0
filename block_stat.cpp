#include "common.h"
#include "file_op.h"
#include "index_handle.h"
#include <sstream>

using namespace qiniu;
using namespace std;

const static largefile::MMapOption mmap_option = {1024000, 4096, 4096};  //�ڴ�ӳ��Ĳ���
const static uint32_t main_blocksize = 1024*1024*64;  //�����ļ��Ĵ�С 
const static uint32_t bucket_size = 1000;  //��ϣͰ�Ĵ�С
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
	
	
	
	//1. ���������ļ�
	largefile::IndexHandle *index_handle = new largefile::IndexHandle(".", block_id);  //�����ļ����
	
	if(debug)  printf("load index ...\n");
	
	ret = index_handle->load(block_id, bucket_size, mmap_option);
	
	if(ret != largefile::TFS_SUCCESS)
	{
		fprintf(stderr, "load index %d failed.\n", block_id);
		//delete mainblock;
		delete index_handle;
		exit(-2);
	}
	
	delete index_handle;
	return 0;
}