#ifndef   QINIU_LARGEFILE_MMAPFILE_H_
#define  QINIU_LARGEFILE_MMAPFILE_H_

#include <unistd.h>
#include "common.h"

namespace qiniu
{
    namespace largefile
	{
		class MMapFile
		{
			
			public:
			  MMapFile();
			  
			  explicit MMapFile(const int fd);
			  MMapFile(const MMapOption& mmap_option,  const int fd);
			  
			  ~MMapFile();
			  
			  bool sync_file();  //ͬ���ڴ����ݵ��ļ�
			  bool map_file(const bool write = false);  //���ļ�ӳ�䵽�ڴ棬ͬʱ���÷���Ȩ��
			  void *get_data() const ;  //��ȡӳ�䵽�ڴ����ݵ��׵�ַ
			  int32_t get_size() const; //��ȡӳ�����ݵĴ�С
			  
			  bool munmap_file();  //���ӳ��
			  bool remap_file();      //����ִ��ӳ��  mremap
			  
			  
			private:
			 bool  ensure_file_size(const  int32_t size);
			 
            private:
			int32_t size_;
			int fd_;
			void *data_;
			
            struct MMapOption mmap_file_option_;		  
		};
		
	}
	
	
}

#endif   //