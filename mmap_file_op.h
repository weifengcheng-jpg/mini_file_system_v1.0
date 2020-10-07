#ifndef QINIU_LARGEFILE_MMAPFILE_OP_H_
#define QINIU_LARGEFILE_MMAPFILE_OP_H_

#include "common.h"
#include "file_op.h"
#include "mmap_file.h"

namespace qiniu
{
	namespace largefile
	{
		class MMapFileOperation: public FileOperation
		{
			
			public:
				MMapFileOperation(const std::string& file_name, const int open_flags = O_CREAT | O_RDWR | O_LARGEFILE):
				FileOperation(file_name, open_flags),  map_file_(NULL),  is_mapped_(false)
				{
					
				}
			
				~MMapFileOperation()
				{
					if(map_file_){
						delete(map_file_);
						map_file_ = NULL;
					}
				}
				
				int mmap_file(const MMapOption& mmap_option);
				int munmap_file();
				
				int pread_file(char *buf, const int32_t size, const int64_t offset);
				int pwrite_file(const char* buf, const int32_t sized, const int64_t offset);
			
				void *get_map_data() const ;
				int flush_file();
			
			private:
				MMapFile *map_file_;
				bool is_mapped_;
		};
	}
}

#endif  //QINIU_LARGEFILE_MMAPFILE_OP_H_