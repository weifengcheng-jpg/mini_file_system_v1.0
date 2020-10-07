
#include "mmap_file.h"
#include "common.h"

using namespace std;
using namespace qiniu;

static const mode_t  OPEN_MODE = 0644;     
const static largefile::MMapOption  mmap_option={10240000, 4096, 4096};  //内存映射的参数


int open_file(string file_name, int open_flags){
	
	int fd = open(file_name.c_str(), open_flags, OPEN_MODE);  //open 成功返回的一定是>0
	if(fd < 0){
		   return -errno;  //errno   strerror(errno);  //read  errno
	}
	
	return fd;
	
}

int main(void ){
	const char *filename = "./mapfile_test.txt";
	
	//1. 打开/创建一个文件，取得文件的句柄   open函数
	int fd = open_file(filename,  O_RDWR  | O_LARGEFILE);
	
	if(fd<0){
		//调用read  ,出错重置 errno 
		fprintf(stderr, "open file failed. filename：%s, error desc: %s\n", filename, strerror(-fd));
		return -1;
	}
	
	largefile::MMapFile *map_file = new largefile::MMapFile(mmap_option, fd);
	
	bool is_mapped = map_file->map_file(true);
	
	if(is_mapped){
		map_file->remap_file();
		
		memset(map_file->get_data(), '9', map_file->get_size());
		map_file->sync_file();
		map_file->munmap_file();
	}else {
			fprintf(stderr, "map file  failed\n");
	}
	
	close(fd);
	
	return 0;
}






