#include "mmap_file_op.h"
#include <iostream>

using namespace std;
using namespace qiniu;

const static largefile::MMapOption  mmap_option={10240000, 4096, 4096};  //�ڴ�ӳ��Ĳ���

int main(void) 
{
	int ret = 0;
	const char * filename = "mmap_file_op.txt";
	largefile::MMapFileOperation   *mmfo = new largefile::MMapFileOperation(filename);
	
	int fd = mmfo->open_file();
	if(fd < 0){
		fprintf(stderr, "open file %s failed. reason: %s\n", filename, strerror(-fd));
		exit(-1);
	}
	
	ret  = mmfo->mmap_file(mmap_option);
	if(ret == largefile::TFS_ERROR)
	{
		fprintf(stderr, "mmap_file failed.  reason: %s\n", strerror(errno));
		mmfo->close_file();
		exit(-2);
	}
	
	
	char buffer[128+1];
	memset(buffer, '6', 128);
	buffer[127]='\n';
	
	ret = mmfo->pwrite_file(buffer, 128, 8);
	if(ret < 0){
		if(ret == largefile::EXIT_DISK_OPER_INCOMPLETE){
			fprintf(stderr, " pwrite_file: read  length is less than required!\n");
		}else {
			fprintf(stderr, "pwrite file %s failed. reason: %s\n", filename, strerror(-ret));
		}
	}
	
	memset(buffer, 0, 128);
	ret = mmfo->pread_file(buffer, 128, 8);
	if(ret < 0){
		if(ret ==  largefile::EXIT_DISK_OPER_INCOMPLETE){
			fprintf(stderr, " pread_file: read  length is less than required!");
		}else {
			fprintf(stderr, "pread file %s failed. reason: %s\n", filename, strerror(-ret));
		}
	}else {
		buffer[128] = '\0';
		printf("read: %s\n", buffer);
	}
	
	ret = mmfo->flush_file();
	if(ret == largefile::TFS_ERROR)
	{
		fprintf(stderr, "flush file failed. reason: %s\n", strerror(errno));
	}
	
	memset(buffer, '8', 128);
	buffer[127]='\n';
	ret = mmfo->pwrite_file(buffer, 128, 4000);
	
	mmfo->munmap_file();
	
	mmfo->close_file();
	return 0;
}
