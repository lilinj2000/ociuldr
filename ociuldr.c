#ifdef RCSID
static char *RCSid =
   "$Header: ociuldr.c 2005.05.19 Lou Fangxin, http://www.anysql.net $ ";
#endif /* RCSID */

/*
   NAME
     ociuldr.c - Using OCI to rewrite the unload script.

   MODIFIED   (MM/DD/YY)
    Lou Fangxin    2005.05.19 -  Initial write.
    Lou Fangxin    2005.05.22 -  Add File Option to command
    Lou Fangxin    2005.05.25 -  Enable login as sysdba
*/

#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <oratypes.h>
#include <ocidfn.h>
#ifdef __STDC__
#include <ociapr.h>
#else
#include <ocikpr.h>
#endif
#include <ocidem.h>

#include <oci.h>

/* Constants used in this program. */
#define MAX_SELECT_LIST_SIZE    512
#define MAX_ITEM_BUFFER_SIZE    33
#define PARSE_NO_DEFER           0
#define PARSE_V7_LNG             2
#define MAX_BINDS               12
#define MAX_SQL_IDENTIFIER      31
#define DEFAULT_BUFFER_SIZE     524288
#define ROW_BATCH_SIZE          500000

#if defined(_WIN32)
#define STRNCASECMP memicmp
#else
#define STRNCASECMP strncasecmp
#endif

#if !defined(MIN)
#define  MIN(a,b) ((a) > (b) ? (b) : (a))
#endif

#if !defined(MAX)
#define  MAX(a,b) ((a) < (b) ? (b) : (a))
#endif

struct COLUMN
{
    /* Describe */
    sb4             dbsize;
    sb2             dbtype;
    sb1             buf[MAX_ITEM_BUFFER_SIZE];
    sb4             buflen;
    sb4             dsize;
    sb2             precision;
    sb2             scale;
    sb2             nullok;

    /*+ Fetch */
    ub1             *colbuf;
    sb2             *indp;
    ub2             *col_retlen;
    ub2             *col_retcode;

    /*+ Point to next column */
    struct          COLUMN *next;
    char fmtstr[64];
};

ub4     DEFAULT_ARRAY_SIZE =   50;
ub4     DEFAULT_LONG_SIZE = 32768;
ub4     hda[HDA_SIZE/(sizeof(ub4))];

FILE *fp_log = NULL;
int return_code = 0;

sword LogonDB(Lda_Def *lda,text *username, text *password,ub4 mode)
{
    sword n;
 
    if (olog(lda, (ub1 *)hda, username, -1, password, -1,
                 (text *) 0, -1, mode))
    {
        fprintf((fp_log == NULL?stdout:fp_log),"Cannot connect as %s.\n", username);
    }
    else
    {
        return 0;
    }
    fprintf((fp_log == NULL?stdout:fp_log),"Connection failed.  Exiting...\n");
    return -1;
}

void SQLError(Lda_Def *lda, Cda_Def *cda)
{
    text msg[512];
    sword n;

    n = oerhms(lda, cda->rc, msg, (sword) sizeof (msg));
    fprintf((fp_log == NULL?stdout:fp_log),"%.*s", n, msg);
}

sword CreateSQL(Lda_Def *lda, Cda_Def *cda)
{
    if (oopen(cda, lda, (text *) 0, -1, -1, (text *) 0, -1))
    {
        fprintf((fp_log == NULL?stdout:fp_log),"Error opening cursor [OOPEN].\n");
        return -1;
    }
    return 0;
}

sword ParseSQL(Lda_Def *lda, Cda_Def *cda, text *sql_statement)
{
    if (oparse(cda, (text *) sql_statement, (sb4) -1,
               (sword) PARSE_NO_DEFER, (ub4) PARSE_V7_LNG))
    {
        SQLError(lda,cda);
        return -1;
    }
    return 0;
}

sword BindValue(Lda_Def *lda, Cda_Def *cda, text *vname, ub1 *val)
{

  if (obndrv(cda, vname, -1, 
            (ub1 *)val,  -1,
            VARCHAR2_TYPE,-1, (sb2 *) 0, (text *) 0, -1, -1))
  {
     SQLError(lda,cda);
     return -1;
  }
  return 0;
}

sword executeStatement(Lda_Def *lda, Cda_Def *cda)
{
     if (oexec(cda))
     {
        SQLError(lda,cda);
        return -1;
     }
     return 0;
}

sword getColumns(FILE *fpctl, Lda_Def *lda,Cda_Def *cda, struct COLUMN *collist, int fixlen)
{
    int totallen=1;
    sword col;
    struct COLUMN *tempcol;
    struct COLUMN *nextcol;
    ub1 *buf;

    nextcol = collist;
 
    /* Describe the select-list items. */
    if(fpctl != NULL) fprintf(fpctl,"(\n");
    for (col = 0; col < MAX_SELECT_LIST_SIZE; col++)
    {
        tempcol = (struct COLUMN *) malloc(sizeof(struct COLUMN));
        tempcol-> indp        = (sb2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(sb2));
        tempcol-> col_retlen  = (ub2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(ub2));
        tempcol-> col_retcode = (ub2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(ub2));

        tempcol->next = NULL;
        tempcol->colbuf = NULL;
	memset(tempcol->fmtstr,0,64);

        tempcol->buflen = MAX_ITEM_BUFFER_SIZE;
        if (odescr(cda, col + 1, &(tempcol->dbsize),
                   &(tempcol->dbtype), &(tempcol->buf[0]),
                   &(tempcol->buflen), &(tempcol->dsize),
                   &(tempcol->precision), &(tempcol->scale),
                   &(tempcol->nullok)))
        {
            if(fpctl != NULL) fprintf(fpctl,"\n");
            free(tempcol);
            /* Break on end of select list. */
            if (cda->rc == VAR_NOT_IN_LIST)
                break;
            else
            {
                SQLError(lda,cda);
                return -1;
            }
        }

        if(col) 
        {
          if(fpctl != NULL) fprintf(fpctl,",\n");
        }

        nextcol->next = tempcol;
        nextcol=tempcol;

        nextcol->buf[nextcol->buflen]='\0';
 
        switch(nextcol->dbtype)
        {
            case DATE_TYPE:
                nextcol->dsize=20;
                nextcol->dbtype=STRING_TYPE;
                if(fpctl != NULL)
                {
                   if (fixlen)
                      fprintf(fpctl,"  %s POSITION(%d:%d) DATE \"YYYY-MM-DD HH24:MI:SS\"", 
                          nextcol->buf , totallen, totallen + nextcol->dsize-1);
                   else
                      fprintf(fpctl,"  %s DATE \"YYYY-MM-DD HH24:MI:SS\"", nextcol->buf );
                }
                break;
            case 180: /* TIMESTAMP */
                nextcol->dsize=30;
                nextcol->dbtype=STRING_TYPE;
                if(fpctl != NULL)
                { 
                   if (fixlen)
                      fprintf(fpctl,"  %s POSITION(%d:%d) TIMESTAMP \"YYYY-MM-DD HH24:MI:SSXFF\"",
                           nextcol->buf , totallen, totallen + nextcol->dsize-1);
                   else
                      fprintf(fpctl,"  %s TIMESTAMP \"YYYY-MM-DD HH24:MI:SSXFF\"", nextcol->buf );
                }
                break;
            case 181: /* TIMESTAMP WITH TIMEZONE */
                nextcol->dsize=36;
                nextcol->dbtype=STRING_TYPE;
                if(fpctl != NULL)
                {
                   if (fixlen)
                       fprintf(fpctl,"  %s POSITION(%d:%d) TIMESTAMP WITH TIME ZONE \"YYYY-MM-DD HH24:MI:SSXFF TZH:TZM\"", 
                           nextcol->buf , totallen, totallen + nextcol->dsize-1);
                   else
                       fprintf(fpctl,"  %s TIMESTAMP WITH TIME ZONE \"YYYY-MM-DD HH24:MI:SSXFF TZH:TZM\"", nextcol->buf );
                }
                break;
            case 24:  /* LONG RAW */
            case 113: /* BLOB */
               nextcol->dsize=DEFAULT_LONG_SIZE;
               nextcol->dbtype=24;
               if(fpctl != NULL)
               {
                  if (fixlen)
                      fprintf(fpctl,"  %s POSITION(%d:%d) CHAR(%d) ", 
                          nextcol->buf, totallen, totallen + nextcol->dsize-1, DEFAULT_LONG_SIZE);
                  else
                      fprintf(fpctl,"  %s CHAR(%d) ", nextcol->buf, DEFAULT_LONG_SIZE);
               }
               break;
            case ROWID_TYPE:
                nextcol->dsize=20;
                nextcol->dbtype=STRING_TYPE;
                if(fpctl != NULL)
                {
                    if (fixlen)
                       fprintf(fpctl, "  %s POSITION(%d:%d) CHAR(%d)", 
                            nextcol->buf, totallen, totallen + nextcol->dsize-1, 20);
                    else
                       fprintf(fpctl, "  %s CHAR(%d)", nextcol->buf,20);
                }
                break;
            case NUMBER_TYPE:
               nextcol->dsize=40;
               nextcol->dbtype=STRING_TYPE;
               if(fpctl != NULL)
               {
                  if (fixlen)
                      fprintf(fpctl,"  %s POSITION(%d:%d) CHAR(%d)", 
                           nextcol->buf,totallen, totallen + nextcol->dsize-1, 40);
                  else
                      fprintf(fpctl,"  %s CHAR(%d)", nextcol->buf,40);
               }
               break;
            case 112: /* CLOB */
            case 114: /* BFILE */
               nextcol->dsize=DEFAULT_LONG_SIZE;
               nextcol->dbtype=STRING_TYPE;
               if(fpctl != NULL)
               {
                  if (fixlen)
                      fprintf(fpctl,"  %s POSITION(%d:%d) CHAR(%d)", 
                          nextcol->buf, totallen, totallen + nextcol->dsize-1, DEFAULT_LONG_SIZE);
                  else
                      fprintf(fpctl,"  %s CHAR(%d)", nextcol->buf,DEFAULT_LONG_SIZE);
               }
               break;
            default:
               nextcol->dbtype=STRING_TYPE;
	       if (nextcol->dsize>4000) nextcol->dsize = 4000;
	       if (nextcol->dsize==0)  nextcol->dsize = 4000;
               if(fpctl != NULL)
               {
                  if (fixlen)
                     fprintf(fpctl,"  %s POSITION(%d:%d) CHAR(%d)", nextcol->buf, 
                            totallen, totallen + nextcol->dsize-1,
                            (nextcol->dsize==0?DEFAULT_LONG_SIZE:nextcol->dsize));
                  else
                     fprintf(fpctl,"  %s CHAR(%d)", nextcol->buf,(nextcol->dsize==0?DEFAULT_LONG_SIZE:nextcol->dsize));
               }
               break;
        }
        /* Set for long type column */
        if (nextcol->dsize > DEFAULT_LONG_SIZE || nextcol->dsize == 0)  
            nextcol->dsize = DEFAULT_LONG_SIZE;
        /* add one more byte to store the ternimal char of string */
	sprintf(nextcol->fmtstr,"%%-%ds", nextcol->dsize);
        totallen = totallen + nextcol->dsize;
        nextcol->dsize ++;

        /* fprintf((fp_log == NULL?stdout:fp_log),"%8u bytes allocated for column %s (%d) \n",
                 DEFAULT_ARRAY_SIZE * nextcol->dsize,nextcol->buf,col+1); */

        nextcol->colbuf = malloc(DEFAULT_ARRAY_SIZE * nextcol->dsize);
        memset(nextcol->colbuf,0,DEFAULT_ARRAY_SIZE * nextcol->dsize);

        if (odefin(cda, col + 1,
                   nextcol->colbuf, nextcol->dsize, nextcol->dbtype,
                   -1, nextcol->indp, (text *) 0, -1, -1,
                   nextcol->col_retlen,nextcol->col_retcode))
        {
            SQLError(lda,cda);
            return -1;
        }
    }
    if(fpctl != NULL)
       fprintf(fpctl,")\n");
    fputs("\n",(fp_log == NULL?stdout:fp_log));
    return col;
}

void  printRowInfo(ub4 row)
{
	time_t now = time(0);
	struct tm *ptm = localtime(&now);
	fprintf((fp_log == NULL?stdout:fp_log),"%8u rows exported at %04d-%02d-%02d %02d:%02d:%02d\n",
                row,
		ptm->tm_year + 1900,
		ptm->tm_mon + 1,
		ptm->tm_mday,
		ptm->tm_hour,
		ptm->tm_min,
		ptm->tm_sec);
        fflush((fp_log == NULL?stdout:fp_log));
}

FILE *openFile(const text *fname, text tempbuf[], int batch)
{
   FILE *fp=NULL;
   int i, j, len;
   time_t now = time(0);
   struct tm *ptm = localtime(&now);   
   
   len = strlen(fname);
   j = 0;
   for(i=0;i<len;i++)
   {
      if (*(fname+i) == '%')
      {
          i++;
	  if (i < len)
	  {
            switch(*(fname+i))
            {
              case 'Y':
              case 'y':
                j += sprintf(tempbuf+j, "%04d", ptm->tm_year + 1900);
		break;
              case 'M':
              case 'm':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mon + 1);
		break;
              case 'D':
              case 'd':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mday);
		break;
              case 'W':
              case 'w':
                j += sprintf(tempbuf+j, "%d", ptm->tm_wday);
		break;
              case 'B':
              case 'b':
                j += sprintf(tempbuf+j, "%d", batch);
		break;
              default:
                tempbuf[j++] = *(fname+i);
		break;
            }
          }
      }
      else
      {
         tempbuf[j++] = *(fname+i);
      }
   }
   tempbuf[j]=0;
   if (tempbuf[0] == '+')
       fp = fopen(tempbuf+1, "ab+");
   else
       fp = fopen(tempbuf, "wb+");
   return fp;
}

void printRow(text *fname, Lda_Def *lda,Cda_Def *cda,struct COLUMN *col, text *field, int flen,text *record, int rlen, int batch, int header, int dispfrm, int fixlen)
{
    int bcount=1,j=0;
    sword r,rows,colcount,c;
    struct COLUMN *p;
    ub4 trows;
    FILE *fp;
    text tempbuf[512];

    struct COLUMN *cols[512];

    trows=0;
    colcount = 0;

    p = col->next;
    while(p != NULL)
    {
        cols[colcount] = p;
        p=p->next;
        colcount ++;
    }

    memset(tempbuf,0,512);

    if((fp = openFile(fname,tempbuf,bcount)) == NULL) 
    {
       fprintf((fp_log == NULL?stdout:fp_log),"ERROR -- Cannot write to file : %s\n", tempbuf);
       return_code = 6;
       return;
    }

    if (header)
    {
       for(c=0;c<colcount;c++)
       {
          if (fixlen)
              fprintf(fp,cols[c]->fmtstr,cols[c]->buf);
          else
          {
              fprintf(fp,"%s",cols[c]->buf);
              if (c < colcount - 1) 
                 fwrite(field,flen,1,fp);
          }
       }
       fwrite(record,rlen,1,fp);
    }

    printRowInfo(trows);
    for (;;)
    {
        rows = DEFAULT_ARRAY_SIZE;
        if (ofen(cda,DEFAULT_ARRAY_SIZE))
        {
            if (cda->rc != NO_DATA_FOUND)
            {
                 return_code = 7;
                 SQLError(lda,cda);
            }
            rows = cda->rpc % DEFAULT_ARRAY_SIZE;
        }
        for(r=0;r<rows;r++)
        {
           for(c=0;c<colcount;c++)
           {
              if (dispfrm)
              {
                  fprintf(fp,"%-31s: ",cols[c]->buf);
              }
              if (*(cols[c]->indp+r) >= 0)
              {
                if (fixlen)
                    fprintf(fp,cols[c]->fmtstr, cols[c]->colbuf+(r* cols[c]->dsize));
                else
                   fwrite(cols[c]->colbuf+(r* cols[c]->dsize),*(cols[c]->col_retlen+r),1,fp);
              }
	      	  else
			  {
			  	//fprintf(fp,cols[c]->fmtstr, " ");
			  }

              if (dispfrm)
              {
              	 fprintf(fp,"\n");
              }
              else
              {
                 if (c < colcount && fixlen == 0)
                     fwrite(field,flen,1,fp);
              }
           }
           fwrite(record,rlen,1,fp);
           trows ++;
           //if (trows % ROW_BATCH_SIZE  == 0)
           if ( batch && (trows%batch==0) )
           {
              printRowInfo(trows);
              //if(batch && ((trows / ROW_BATCH_SIZE) % batch) == 0)
              {
                 fprintf((fp_log == NULL?stdout:fp_log),"         output file %s closed at %u rows.\n", tempbuf, trows);
                 fclose(fp);
                 bcount ++;
                 memset(tempbuf,0,512);
                 if((fp = openFile(fname,tempbuf,bcount)) == NULL) 
                 {
                   fprintf((fp_log == NULL?stdout:fp_log),"ERROR -- Cannot write to file : %s\n", tempbuf);
                   return_code = 6;
                   return;
                 }
                 if (header)
                 {
                    for(c=0;c<colcount;c++)
                    {
                       if (fixlen)
                           fprintf(fp,cols[c]->fmtstr,cols[c]->buf);
                       else
                       {
                           fprintf(fp,"%s",cols[c]->buf);
                           if (c < colcount - 1)
                              fwrite(field,flen,1,fp);
                       }
                    }
                    fwrite(record,rlen,1,fp);
                 }
                 trows = 0;
              }
           }
        }
        if (rows < DEFAULT_ARRAY_SIZE) break;
    }
    //if (trows % ROW_BATCH_SIZE != 0)
       printRowInfo(trows);
    fclose(fp);
    fprintf((fp_log == NULL?stdout:fp_log),"         output file %s closed at %u rows.\n\n", tempbuf, trows);
    fflush((fp_log == NULL?stdout:fp_log));
}

void freeColumn(struct COLUMN *col)
{
   struct COLUMN *p,*temp;
   p=col->next;

   col->next = NULL;
   while(p!=NULL)
   {
     free(p->colbuf);
     free(p->indp);
     free(p->col_retlen);
     free(p->col_retcode);
     temp=p;
     p=temp->next;
     free(temp);
   }
}

ub1  getHexIndex(char c)
{
   if ( c >='0' && c <='9') return c - '0';
   if ( c >='a' && c <='f') return 10 + c - 'a';
   if ( c >='A' && c <='F') return 10 + c - 'A';
   return 0;
}

int convertOption(const ub1 *src, ub1* dst, int mlen)
{
   int i,len,pos;
   ub1 c,c1,c2;

   i=pos=0;
   len = strlen(src);
   
   
   while(i<MIN(mlen,len))
   {
      if ( *(src+i) == '0')
      {
          if (i < len - 1)
          {
             c = *(src+i + 1);
             switch(c)
             {
                 case 'x':
                 case 'X':
                   if (i < len - 3)
                   {
                       c1 = getHexIndex(*(src+i + 2));
                       c2 = getHexIndex(*(src+i + 3));
                       *(dst + pos) = (ub1)((c1 << 4) + c2);
                       i=i+2;
                   }
                   else if (i < len - 2)
                   {
                       c1 = *(src+i + 2);
                       *(dst + pos) = c1;
                       i=i+1;
                   }
                   break;
                 default:
                   *(dst + pos) = c;
                   break;
             }
             i = i + 2;
          }
          else
          {
            i ++;
          }
      }
      else
      {
          *(dst + pos) = *(src+i);
          i ++;
      }
      pos ++;
   }
   *(dst+pos) = '\0';
   return pos;
}

int main(int argc, char *argv[])
{

    struct COLUMN col;

    sword n,i,argcount=0;
    Lda_Def conn;
    Cda_Def stmt;
    
    int  frmdisp=0;
    ub1 *iobuf;
    text tempbuf[512];
    text username[132]="";
    text ctlfname[256]="";
    text tabname[132]="";
    text tabmode[132]="INSERT";
    text query[32768]="";
    text field[132]=",";
    text logfile[256]="";
    text record[132]="\n";
    text sqlfname[255]="";
    text fname[255]="uldrdata.txt";
    text argname[128][20];
    ub1  argval [128][20];

    int  buffer= 16777216;
    int  hsize = 0;
    int  ssize = 0;
    int  bsize = 0;
    int  serial= 0;
    int  trace = 0;
    int  batch = 0;
    int  header= 0;

    ub4  lmode;

    int flen,rlen;

    FILE *fp;
    FILE *fpctl;

    flen = rlen = 1;
    col.next=NULL;
    lmode = OCI_LM_DEF;

    for(i=0;i<argc;i++)
    {
      if (STRNCASECMP("user=",argv[i],5)==0)
      {
          memset(username,0,132);
          memcpy(username,argv[i]+5,MIN(strlen(argv[i]) - 5,131));
      }
      else if (STRNCASECMP("query=",argv[i],6)==0)
      {
          memset(query,0,8192);
          memcpy(query,argv[i]+6,MIN(strlen(argv[i]) - 6,8191));
      }     
      else if (STRNCASECMP("field=",argv[i],6)==0)
      {
          memset(field,0,132);
          flen=convertOption(argv[i]+6,field,MIN(strlen(argv[i]) - 6,131));
      }     
      else if (STRNCASECMP("sql=",argv[i],4)==0)
      {
          memset(sqlfname,0,132);
          memcpy(sqlfname,argv[i]+4,MIN(strlen(argv[i]) - 4,254));
      }
      else if (STRNCASECMP("record=",argv[i],7)==0)
      {
          memset(record,0,132);
          rlen=convertOption(argv[i]+7,record,MIN(strlen(argv[i]) - 7,131));
      }     
      else if (STRNCASECMP("file=",argv[i],5)==0)
      {
          memset(fname,0,132);
          memcpy(fname,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
      }     
      else if (STRNCASECMP("log=",argv[i],4)==0)
      {
          memset(logfile,0,256);
          memcpy(logfile,argv[i]+4,MIN(strlen(argv[i]) - 4,254));
      }     
      else if (STRNCASECMP("table=",argv[i],6)==0)
      {
          memset(tabname,0,132);
          memcpy(tabname,argv[i]+6,MIN(strlen(argv[i]) - 6,128));
      }     
      else if (STRNCASECMP("mode=",argv[i],5)==0)
      {
          memset(tabmode,0,132);
          memcpy(tabmode,argv[i]+5,MIN(strlen(argv[i]) - 5,128));
      }     
      else if (STRNCASECMP("head=",argv[i],5)==0)
      {
          memset(tempbuf,0,132);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,128));
          header = 0;
          if (STRNCASECMP(tempbuf,"YES",3) == 0) header = 1;
          if (STRNCASECMP(tempbuf,"ON",3) == 0) header = 1;
      }     
      else if (STRNCASECMP("sort=",argv[i],5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
          ssize = atoi(tempbuf);
          if (ssize < 0) ssize = 0;
          if (ssize > 512) ssize = 512;
      }     
      else if (STRNCASECMP("buffer=",argv[i],7)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+7,MIN(strlen(argv[i]) - 7,254));
          buffer = atoi(tempbuf);
          if (buffer < 8) buffer = 8;
          if (ssize > 100) buffer = 100;
          buffer = buffer * 1048576;
      }    
      else if (STRNCASECMP("form=",argv[i],5)==0)
      {
          memset(tempbuf,0,132);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,128));
          frmdisp=0;
          if (STRNCASECMP(tempbuf,"YES",3) == 0) frmdisp = 1;
          if (STRNCASECMP(tempbuf,"ON",3) == 0) frmdisp = 1;
      }       
      else if (STRNCASECMP("long=",argv[i],5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
          DEFAULT_LONG_SIZE = atoi(tempbuf);
          if (DEFAULT_LONG_SIZE < 100) DEFAULT_LONG_SIZE = 100;
          if (DEFAULT_LONG_SIZE > 32767) DEFAULT_LONG_SIZE = 32767;
      }
      else if (STRNCASECMP("array=",argv[i],6)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
          DEFAULT_ARRAY_SIZE = atoi(tempbuf);
          if (DEFAULT_ARRAY_SIZE < 5) DEFAULT_ARRAY_SIZE = 5;
          if (DEFAULT_ARRAY_SIZE > 2000) DEFAULT_ARRAY_SIZE = 2000;
      }
      else if (STRNCASECMP("hash=",argv[i],5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
          hsize = atoi(tempbuf);
          if (hsize < 0) hsize = 0;
          if (hsize > 512) hsize = 512;
      }     
      else if (STRNCASECMP("read=",argv[i],5)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
          bsize = atoi(tempbuf);
          if (bsize < 0) bsize = 0;
          if (bsize > 512) bsize = 512;
      }     
      else if (STRNCASECMP("batch=",argv[i],6)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
          batch = atoi(tempbuf);
          if (batch < 0) batch = 0;
          if (batch == 1) batch = 2;
      }     
      else if (STRNCASECMP("serial=",argv[i],7)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+7,MIN(strlen(argv[i]) - 7,254));
          serial = atoi(tempbuf);
      }     
      else if (STRNCASECMP("trace=",argv[i],6)==0)
      {
          memset(tempbuf,0,512);
          memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
          trace = atoi(tempbuf);
      }     
      else if (STRNCASECMP("-si",argv[i],3) == 0 && strlen(argv[i])==3)
      {
          lmode = lmode | OCI_SYSDBA;
          memset(username,0,132);
		  username[0]='/' ;
		  //strcpy(username, "/ as sysdba") ;
      }
    }
    
    if (strlen(sqlfname) > 0)
    {
       fp = fopen(sqlfname,"r+");
       if (fp != NULL)
       {
           while(!feof(fp))
           {
               memset(tempbuf,0,512);
               fgets(tempbuf,1023,fp);
               strcat(query,tempbuf);
               strcat(query," ");
           }
           fclose(fp);
       }
    }
    
    if (strlen(username)==0 || strlen(query)==0)
    {
       printf("Usage: %s user=... query=... field=... record=... file=...\n",argv[0]);
       printf("Notes:\n");
       printf("       -si   = enable logon as SYSDBA\n");
       printf("       user  = username/password@tnsname\n");
       printf("       sql   = SQL file name\n");
       printf("       query = select statement\n");
       printf("       field = seperator string between fields\n");
       printf("       record= seperator string between records\n");
       printf("       head  = print row header(Yes|No)\n");
       printf("       batch = any number larger than 1(file parameter should like filename%%b.txt)\n");
       printf("       file  = output file name(default: uldrdata.txt)\n");
       printf("       read  = set DB_FILE_MULTIBLOCK_READ_COUNT at session level\n");
       printf("       sort  = set SORT_AREA_SIZE at session level (UNIT:MB) \n");
       printf("       hash  = set HASH_AREA_SIZE at session level (UNIT:MB) \n");
       printf("       serial= set _serial_direct_read to TRUE at session level\n");
       printf("       trace = set event 10046 to given level at session level\n");
       printf("       table = table name in the sqlldr control file\n");
       printf("       mode  = sqlldr option, INSERT or APPEND or REPLACE or TRUNCATE \n");
       printf("       log   = log file name, prefix with + to append mode\n");
       printf("       long  = maximum long field size\n");
       printf("       array = array fetch size\n");
       printf("       buffer= sqlldr READSIZE and BINDSIZE, default 16 (MB)\n");
       printf("       form  = display rows as form (yes or no)\n");
       printf("\n");
       printf("  for field and record, you can use '0x' to specify hex character code,\n");
       printf("  \\r=0x%02x \\n=0x%02x |=0x%0x ,=0x%02x \\t=0x%02x\n",'\r','\n','|',',','\t');
       exit(0);
    }

    if (DEFAULT_ARRAY_SIZE * DEFAULT_LONG_SIZE > 104857600)
    {
        DEFAULT_ARRAY_SIZE = 104857600/DEFAULT_LONG_SIZE;
        if (DEFAULT_ARRAY_SIZE < 5) DEFAULT_ARRAY_SIZE=5;
    }

    if (strlen(logfile))
    {
       fp_log = openFile(logfile,tempbuf,0);
    }

    if(LogonDB(&conn,username,NULL,lmode))
    {
       if (fp_log != NULL) fclose(fp_log);
       exit(1);
    }

    if(CreateSQL(&conn,&stmt) == 0)
    { 

        ParseSQL(&conn,&stmt,"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
        executeStatement(&conn,&stmt);
        ParseSQL(&conn,&stmt,"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SSXFF'");
        executeStatement(&conn,&stmt);
        ParseSQL(&conn,&stmt,"ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SSXFF TZH:TZM'");
        executeStatement(&conn,&stmt);

        if (bsize)
        {
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT=%d",bsize);
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
        }
        if (hsize)
        {
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET HASH_AREA_SIZE=%d",hsize * 1048576);
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET \"_hash_multiblock_io_count\"=128");
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
        }
        if (serial)
        {
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET \"_serial_direct_read\"=TRUE");
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
        }
        if (ssize)
        {
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET SORT_AREA_SIZE=%d",ssize * 1048576);
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET SORT_AREA_RETAINED_SIZE=%d",ssize * 1048576);
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET \"_sort_multiblock_read_count\"=128");
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
        }
        if (trace)
        {
	   memset(tempbuf,0,512);
           sprintf(tempbuf,"ALTER SESSION SET EVENTS='10046 TRACE NAME CONTEXT FOREVER,LEVEL %d'", trace);
           ParseSQL(&conn,&stmt,tempbuf);
           executeStatement(&conn,&stmt);
        }
        if(ParseSQL(&conn,&stmt,query) == 0)
        {
            /*
               Pass parameter value here
            */
            for(i=0;i<argc;i++)
            {
               if (STRNCASECMP("arg:",argv[i],4) == 0)
               {        
                  memset(argname[argcount],0,128);
                  memset(argval[argcount] ,0,128);
                  memset(tempbuf,0,512);
                  memcpy(tempbuf,argv[i]+4,MIN(strlen(argv[i]) - 4,254));
                  for(n=0;n<strlen(tempbuf);n++) if (tempbuf[n]=='=') break;
                  memcpy(argname[argcount], tempbuf, n);
                  if(n+1<strlen(tempbuf)) memcpy(argval[argcount], tempbuf+n+1, strlen(tempbuf) - n - 1);
                  if(strlen(argname[argcount])>0)
		  {
			BindValue(&conn,&stmt,argname[argcount],argval[argcount]);
			argcount ++;
		  }
		  if (argcount >= 20) break;
               }
            }

            if(executeStatement(&conn,&stmt) == 0)
            {
                if(strlen(tabname))
                {
                    memset(ctlfname,0,256);
                    sprintf(ctlfname,"%s_sqlldr.ctl",tabname);
                    fpctl = fopen(ctlfname,"wb+");
                    
                    if(fpctl != NULL)
                    {
                       fprintf(fpctl,"--\n");
                       fprintf(fpctl,"-- Generated by OCIULDR\n");
                       fprintf(fpctl,"--\n");
                       if (!header)
                          fprintf(fpctl,"OPTIONS(BINDSIZE=%d,READSIZE=%d,ERRORS=-1,ROWS=50000)\n", buffer, buffer);
                       else
                          fprintf(fpctl,"OPTIONS(BINDSIZE=%d,READSIZE=%d,SKIP=1,ERRORS=-1,ROWS=50000)\n", buffer, buffer);
                       fprintf(fpctl,"LOAD DATA\n");
                       fprintf(fpctl,"INFILE '%s' \"STR X'", fname);
                       for(i=0;i<strlen(record);i++) fprintf(fpctl,"%02x",record[i]);
                       fprintf(fpctl,"'\"\n");
                       fprintf(fpctl,"%s INTO TABLE %s\n", tabmode, tabname);
                       fprintf(fpctl,"FIELDS TERMINATED BY X'");
                       for(i=0;i<strlen(field);i++) fprintf(fpctl,"%02x",field[i]);
                       fprintf(fpctl,"' TRAILING NULLCOLS \n");
                    }
                }
                if(getColumns(fpctl,&conn,&stmt,&col, (field[0]==0x20?1:0)) > 0)
                {
                      /*
                      iobuf = (text *)malloc(DEFAULT_BUFFER_SIZE);
                      memset(iobuf,0,DEFAULT_BUFFER_SIZE);
                      setvbuf(fp,iobuf,_IOFBF,DEFAULT_BUFFER_SIZE);
                      printRow(fp,&conn,&stmt,&col,field,flen,record,rlen);
                      fclose(fp);
                      free(iobuf);
                      */
                      printRow(fname,&conn,&stmt,&col,field,flen,record,rlen,batch, header, frmdisp, (field[0]==0x20?1:0));
                }
                else
                {
                      return_code = 5;
                }
                if (fpctl != NULL) fclose(fpctl);
             }
             else
             {
                return_code = 4;
             }
        }
        else
        {
            return_code = 3;
        }
      	oclose(&stmt);
    } 
    else
    {
	return_code = 2;
    }

    freeColumn(&col);
    ologof(&conn);
    if (fp_log != NULL) fclose(fp_log);
    
    return return_code;
}
