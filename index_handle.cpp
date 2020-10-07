#include "common.h"
#include "index_handle.h"
#include <sstream>

static int debug = 1;

namespace qiniu
{
	namespace largefile
	{
		
		IndexHandle::IndexHandle(const std::string& base_path, const uint32_t main_block_id)
		{
			//create file_op_ handle object
			std::stringstream tmp_stream;
			tmp_stream << base_path<< INDEX_DIR_PREFIX<< main_block_id;			//    /root/martin/index/1
			
			std::string index_path;
			tmp_stream>>index_path;
			
			file_op_ = new MMapFileOperation(index_path,  O_CREAT | O_RDWR | O_LARGEFILE);
			is_load_ = false;
		}
		
		IndexHandle::~IndexHandle()
		{
			if(file_op_)
			{
				delete file_op_;
				file_op_ = NULL;
			}
		}
		
		int IndexHandle::create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(debug) printf(" create index , block id: %u , bucket size: %d, max_mmap_size: %d, first mmap size: %d,  per mmap size: %d\n",
										logic_block_id, bucket_size,  map_option.max_mmap_size_, map_option.first_mmap_size_, map_option.per_mmap_size_);
			
			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();
			
			if(file_size < 0)
			{
				return TFS_ERROR;
			}else if(file_size == 0) //empty file
			{
				IndexHeader i_header;
				i_header.block_info_.block_id_ = logic_block_id;
				i_header.block_info_.seq_no_ = 1;
				i_header.bucket_size_ = bucket_size;
				
				i_header.index_file_size_ = sizeof(IndexHeader) + bucket_size * sizeof(int32_t);
				
				//index header + total buckets
				char * init_data = new char[i_header.index_file_size_];
				memcpy(init_data, &i_header, sizeof(IndexHeader));
				memset(init_data + sizeof(IndexHeader), 0,  i_header.index_file_size_ - sizeof(IndexHeader));
				
				//write index header and buckets into index file 
				ret = file_op_->pwrite_file(init_data, i_header.index_file_size_, 0);
				
				delete[] init_data;
				init_data = NULL;
				
				if(ret != TFS_SUCCESS ){
					return ret;
				}
				
				ret = file_op_->flush_file();
				
				if(ret != TFS_SUCCESS ){
					return ret;
				}
				
			}else //file size > 0 , index already exist
			{
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}
			
			ret = file_op_->mmap_file(map_option);
			if(ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			is_load_ = true;
			
			if(debug) printf("init blockid:  %d index successful. data file size: %d, index file size: %d, bucket_size: %d, free head offset: %d, seqno: %d, size: %d, filecount: %d, del_size: %d, del_file_count: %d version: %d\n",
									logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_,
									index_header()->bucket_size_, index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_,
									block_info()->file_count_, block_info()->del_size_, block_info()->del_file_count_, block_info()->version_);
			
			return TFS_SUCCESS;
		}
		
		
		int IndexHandle::load(const uint32_t logic_block_id, const int32_t _bucket_size, const MMapOption map_option)
		{
			int ret = TFS_SUCCESS;
			
			if(is_load_)
			{
				return EXIT_INDEX_ALREADY_LOADED_ERROR;
			}
			
			int64_t file_size = file_op_->get_file_size();
			if(file_size <0)
			{
				return file_size;
			}else if(file_size == 0)//empty file 
			{
				return  EXIT_INDEX_CORRUPT_ERROR;
			}
			
			MMapOption tmp_map_option = map_option;
			
			if(file_size> tmp_map_option.first_mmap_size_ && file_size <= tmp_map_option.max_mmap_size_)
			{
				tmp_map_option.first_mmap_size_ = file_size;
			}
			
			ret = file_op_->mmap_file(tmp_map_option);
			
			if( ret != TFS_SUCCESS)
			{
				return ret;
			}
			
			if(debug) printf("IndexHandle::load - bucket_size(): %d, index_header()->bucket_size_: %d,  block id: %d\n", bucket_size(), index_header()->bucket_size_, block_info()->block_id_);
			
			if(0 == bucket_size() || 0 == block_info()->block_id_)
			{
				fprintf(stderr, "Index corrupt error. blockid: %u, bucket size: %d\n", block_info()->block_id_, bucket_size());
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check file size
			int32_t index_file_size = sizeof(IndexHeader) + bucket_size() * sizeof(int32_t);
			
			if(file_size < index_file_size)
			{
				fprintf(stderr, "Index corrupt error,blockid: %u, bucket size: %d, file size: %ld, index file size: %d\n", block_info()->block_id_, bucket_size(), file_size,index_file_size);
				return EXIT_INDEX_CORRUPT_ERROR;
			}
			
			//check block id 
			if(logic_block_id != block_info()->block_id_)
			{
				fprintf(stderr, "block id conflict. blockid: %u, index blockid： %u\n", logic_block_id, block_info()->block_id_);
				return EXIT_BLOCKID_CONFLICT_ERROR;
			}
			
			//check bucket_size
			if(_bucket_size != bucket_size())
			{
				fprintf(stderr, "Index configure error, old bucket size: %d, new bucket size: %d\n", bucket_size(), _bucket_size);
				return EXIT_BUCKET_CONFIGURE_ERROR;
			}
			
			is_load_ = true;
			
			if(debug) printf("load blockid:  %d index successful. data file size: %d, index file size: %d, bucket_size: %d, free head offset: %d, seqno: %d, size: %d, filecount: %d, del_size: %d, del_file_count: %d version: %d\n",
									logic_block_id, index_header()->data_file_offset_, index_header()->index_file_size_,
									index_header()->bucket_size_, index_header()->free_head_offset_, block_info()->seq_no_, block_info()->size_,
									block_info()->file_count_, block_info()->del_size_, block_info()->del_file_count_, block_info()->version_);
		
			return TFS_SUCCESS;
		}
		
		int IndexHandle::remove(const uint32_t logic_block_id)
		{
			if(is_load_){
				if(logic_block_id != block_info()->block_id_)
				{
					fprintf(stderr, "block id conflict. bolckid: %d, index blokid: %d\n", logic_block_id,  block_info()->block_id_);
					return EXIT_BLOCKID_CONFLICT_ERROR;
				}
			}
			
			int ret = file_op_->munmap_file();
			if( TFS_SUCCESS != ret)
			{
				return ret;
			}
			
			ret = file_op_->unlink_file();
			return ret;
		}
		
		int IndexHandle::flush()
		{
			int  ret = file_op_->flush_file();
			
			if(TFS_SUCCESS != ret)
			{
				fprintf(stderr, "index flush fail , ret: %d error desc: %s\n", ret, strerror(errno));
			}
			
			return ret;
		}
		
		int IndexHandle::update_block_info(const OperType oper_type, const uint32_t modify_size)
		{
				if( block_info()->block_id_ == 0)
				{
					return EXIT_BLOCKID_ZERO_ERROR;
				}
				
				if( oper_type  == C_OPER_INSERT )
				{
					++block_info()->version_;
					++block_info()->file_count_;
					++block_info()->seq_no_;
					block_info()->size_ += modify_size;
				}else if( oper_type == C_OPER_DELETE )
				{
					++block_info()->version_;
					--block_info()->file_count_;
					block_info()->size_ -= modify_size;
					++block_info()->del_file_count_;
					block_info()->del_size_ += modify_size;
				}
				
				if(debug) printf("update block info. blockid: %u, version: %u, file count: %u, size: %u, del file count: %u, del size: %u, seq no: %u, oper type: %d\n",
								block_info()->block_id_, block_info()->version_, block_info()->file_count_, block_info()->size_,
								block_info()->del_file_count_, block_info()->del_size_, block_info()->seq_no_, oper_type);
				
				return TFS_SUCCESS;
		}
		
		int IndexHandle::write_segment_meta(const uint64_t key, MetaInfo &meta)
		{
			int32_t  current_offset = 0, previous_offset = 0;
			
			//思考？ key  存在吗？ 存在=>处理？ 不存在=>处理？
			//1.从文件哈希表中查找key 是否存在  hash_find(key, current_offset, previous_offset);
			int ret = hash_find(key, current_offset, previous_offset);
			if( TFS_SUCCESS == ret)
			{
				return EXIT_META_UNEXPECT_FOUND_ERROR;
			}else if( EXIT_META_NOT_FOUND_ERROR != ret)
			{
					return ret;
			}
			
			//2.不存在就写入meta 到文件哈希表中  hash_insert(key, previous_offset, meta);
			ret = hash_insert(key,  previous_offset, meta);
			return ret;
		}
		
		
		int IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t&  previous_offset)
		{
			int ret = TFS_SUCCESS;
			MetaInfo meta_info;
			
			current_offset = 0;
			previous_offset = 0;
			
			//1.确定key 存放的桶(slot)的位置
			int32_t slot = static_cast<uint32_t>(key)  % bucket_size();
			
			//2.读取桶首节点存储的第一个节点的偏移量，如果偏移量为零，直接返回 EXIT_META_NOT_FOUND_ERROR
			//3.根据偏移量读取存储的metainfo 
			//4.与key进行比较，相等则设置current_offset 和previous_offset 并返回TFS_SUCCESS,否则继续执行5
			//5. 从metainfo 中取得下一个节点的在文件中的偏移量,如果偏移量为零，直接返回 EXIT_META_NOT_FOUND_ERROR,
			//    否则，跳转至3继续循环执行
			
			
			int32_t pos = bucket_slot()[slot];
			
			for(;pos != 0; )
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&meta_info), sizeof(MetaInfo), pos);
				if( TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				if( hash_compare( key, meta_info.get_key() ) ){
					current_offset = pos;
					return TFS_SUCCESS;
				}
				
				previous_offset = pos;
				pos = meta_info.get_next_meta_offset();
				
			}
			
			return EXIT_META_NOT_FOUND_ERROR;
			
		}
		
		int32_t IndexHandle::read_segment_meta(const uint64_t key, MetaInfo &meta)
		{
			int32_t  current_offset = 0, previous_offset = 0;
			
			//1.确定key 存放的桶(slot)的位置
			//int32_t slot = static_cast<uint32_t>(key)  % bucket_size();
			
			int32_t ret = hash_find(key,  current_offset, previous_offset);
			
			if( TFS_SUCCESS == ret) //exist
			{
				ret = file_op_->pread_file(reinterpret_cast<char *> (&meta), sizeof(MetaInfo), current_offset);
				return ret;
			}else 
			{
				return ret;
			}
		}
		
		int32_t IndexHandle::delete_segment_meta(const uint64_t key)
		{
			int32_t  current_offset = 0, previous_offset = 0;
			
			int32_t ret = hash_find(key,  current_offset, previous_offset);
			if( ret != TFS_SUCCESS )
			{
				return ret;
			}
			
			MetaInfo meta_info;
			
			ret = file_op_->pread_file(reinterpret_cast<char *>(&meta_info), sizeof(MetaInfo), current_offset);
			if ( TFS_SUCCESS != ret )
			{
				return ret;
			}
			
			int32_t next_pos = meta_info.get_next_meta_offset();
			
			if(previous_offset == 0)
			{
				int32_t slot = static_cast<uint32_t> (key) % bucket_size();
				bucket_slot()[slot] = next_pos;
			}else {
				MetaInfo pre_meta_info;
				ret = file_op_->pread_file(reinterpret_cast<char *>(&pre_meta_info), sizeof(MetaInfo), previous_offset);
				if ( TFS_SUCCESS != ret )
				{
					return ret;
				}
				
				pre_meta_info.set_next_meta_offset(next_pos);
				
				ret = file_op_->pwrite_file(reinterpret_cast<char *>(&pre_meta_info), sizeof(MetaInfo), previous_offset);
				if ( TFS_SUCCESS != ret )
				{
					return ret;
				}
			}
			
			//把删除节点加入可重用节点链表（下一个小节实现）
			meta_info.set_next_meta_offset(free_head_offset());// index_header()->free_head_offset_;
			ret = file_op_->pwrite_file(reinterpret_cast<char *>(&meta_info), sizeof(MetaInfo),  current_offset);
			if( TFS_SUCCESS != ret )
			{
				return  ret;
			}
			
			index_header()->free_head_offset_ = current_offset;
			if(debug) printf("delete_segment_meta - reuse metainfo, current_offset: %d\n", current_offset);
			
			update_block_info(C_OPER_DELETE, meta_info.get_size());
			return TFS_SUCCESS;
			
		}
		
		
		int32_t IndexHandle::hash_insert(const uint64_t key, int32_t previous_offset, MetaInfo &meta)
		{
			int  ret = TFS_SUCCESS;
			MetaInfo tmp_meta_info;
			int32_t current_offset = 0;
			
			//1.确定key 存放的桶(slot)的位置
			int32_t slot = static_cast<uint32_t>(key) % bucket_size();
			
			//2.确定meta 节点存储在文件中的偏移量
			if(free_head_offset() != 0)
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), free_head_offset());
				if( TFS_SUCCESS != ret)
				{
					return ret;
				}
				
				current_offset = index_header()->free_head_offset_;
				if(debug) printf("reuse metainfo, current_offset: %d\n", current_offset);
				index_header()->free_head_offset_ = tmp_meta_info.get_next_meta_offset();
			}else {
				current_offset = index_header()->index_file_size_;
				index_header()->index_file_size_ += sizeof(MetaInfo);
			}
			
			//3.将meta 节点写入索引文件中
			meta.set_next_meta_offset(0);
			
			ret = file_op_->pwrite_file( reinterpret_cast<const char*>(&meta), sizeof(MetaInfo), current_offset);
			if( TFS_SUCCESS != ret)
			{
				index_header()->index_file_size_ -= sizeof(MetaInfo);
				return ret;
			}
			
			//4. 将meta 节点插入到哈希链表中
			
			//当前一个节点已经存在
			if( 0 != previous_offset)
			{
				ret = file_op_->pread_file(reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
				if( TFS_SUCCESS != ret)
				{
					index_header()->index_file_size_ -= sizeof(MetaInfo);
					return ret;
				}
				
				tmp_meta_info.set_next_meta_offset(current_offset);
				ret = file_op_->pwrite_file( reinterpret_cast<const char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset);
				if( TFS_SUCCESS != ret)
				{
					index_header()->index_file_size_ -= sizeof(MetaInfo);
					return ret;
				}
			}else  { //不存在前一个节点的情况
				bucket_slot()[slot] = current_offset;
			}
			
			return TFS_SUCCESS;
		}
	}
}