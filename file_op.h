#ifndef QINIU_LARGE_FILE_OP_H_
#define QINIU_LARGE_FILE_OP_H_

#include "common.h"

namespace qiniu
{
		namespace largefile
		{
				class FileOperation
				{
				public:
					FileOperation(const std::string &file_name, const int open_flags = O_RDWR | O_LARGEFILE);
					~FileOperation();
					
					int open_file();
					void close_file();
					
					int flush_file();  //���ļ�����д�뵽����
					
					int unlink_file(); 
					
					virtual int pread_file(char *buf, const int32_t nbytes, const int64_t offset);
					virtual int pwrite_file(const char *buf,  const int32_t nbytes, const  int64_t offset);  
					
					int write_file(const char *buf, const int32_t nbytes);//seek
					
					int64_t get_file_size();
					
					int ftruncate_file(const int64_t length);
					int seek_file(const int64_t offset);
					
					int get_fd() const 
					{
						return fd_;
					}
					
					protected:
						int fd_;
						int open_flags_;
						char *file_name_;
						
						protected:
						int check_file();
						
					protected:
						static const mode_t  OPEN_MODE = 0644;
						static const int MAX_DISK_TIMES = 5;
	
				};
		}
}

#endif  //QINIU_LARGE_FILE_OP_H_