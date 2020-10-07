#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>

int main(int argc,char *argv[])
{
    int fd = open("pread.txt",O_RDWR|O_CREAT,0777);
    int num = write(fd,"Hello World!\n",strlen("Hello World!\n"));
    if (num < 0)
    {
        perror("write error!\n");
        return -1;
    }
    int offset = lseek(fd,0,SEEK_CUR);
    printf("num = %d,offset = %d\n",num,offset); // num = 13,offset = 13;
    
    pwrite(fd,"My Best Friends!",strlen("My Best Friends!"),6);
    offset = lseek(fd,0,SEEK_CUR);
    printf("after pwrite, offset = %d\n", offset); // offset = 13;
    
    char buf[20] = "",buf1[20] = "";
    int ret = read(fd,buf,sizeof(buf));
    if (ret < 0)
    {
        perror("read error!\n");
        return -1;
    }
    
    int offset1 = lseek(fd,0,SEEK_CUR);
    printf("ret = %d,offset1 = %d\n",ret,offset1); // ret = 9,offset1 = 22;
    
    pread(fd,buf1,sizeof(buf1),6);
    printf("buf = %s,buf1 = %s\n",buf,buf1);// buf =  Friends!,buf1 = My Best Friends!
    
    return 0;
}
