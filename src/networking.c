/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>

static void setProtocolError(const char *errstr, client *c, int pos);

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdsZmallocSize(o->ptr);
    case OBJ_ENCODING_EMBSTR: return zmalloc_size(o)-sizeof(robj);
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    return sdsdup(o);
}

void freeClientReplyValue(void *o) {
    sdsfree(o);
}

/*
 * 订阅模式对比函数
 */
int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

/*
 * 创建一个新客户端
 */
client *createClient(int fd) {
    // 分配空间
    client *c = zmalloc(sizeof(client));

    /* passing -1 as fd it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    // 当 fd 不为 -1 时，创建带网络连接的客户端
    // 如果 fd 为 -1 ，那么创建无网络连接的伪客户端
    // 因为 Redis 的命令必须在客户端的上下文中使用，所以在执行 Lua 环境中的命令时
    // 需要用到这种伪终端
    if (fd != -1) {
        // 非阻塞
        anetNonBlock(NULL,fd);
        // 禁用 Nagle 算法
        anetEnableTcpNoDelay(NULL,fd);
        // 设置 keep alive
        if (server.tcpkeepalive)
            anetKeepAlive(NULL,fd,server.tcpkeepalive);
        // 绑定读事件到事件 loop （开始接收命令请求）
        if (aeCreateFileEvent(server.el,fd,AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            close(fd);
            zfree(c);
            return NULL;
        }
    }

    // 初始化各个属性

    // 默认数据库
    selectDb(c,0);
    c->id = server.next_client_id++;
    c->fd = fd;
    // 名字
    c->name = NULL;
    // 回复缓冲区的偏移量
    c->bufpos = 0;
    // 查询缓冲区
    c->querybuf = sdsempty();
    c->pending_querybuf = sdsempty();
    // 查询缓冲区峰值
    c->querybuf_peak = 0;
    // 命令请求的类型
    c->reqtype = 0;
    // 命令参数数量
    c->argc = 0;
    // 命令参数
    c->argv = NULL;
    // 当前执行的命令和最近一次执行的命令
    c->cmd = c->lastcmd = NULL;
    // 查询缓冲区中未读入的命令内容数量
    c->multibulklen = 0;
    // 读入的参数的长度
    c->bulklen = -1;
    // 已发送字节数
    c->sentlen = 0;
    // 状态 FLAG
    c->flags = 0;
    // 创建时间和最后一次互动时间
    c->ctime = c->lastinteraction = server.unixtime;
    // 认证状态
    c->authenticated = 0;
    // 复制状态
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    // 复制偏移量
    c->reploff = 0;
    c->read_reploff = 0;
    // 通过 ACK 命令接收到的偏移量
    c->repl_ack_off = 0;
    // 通过 AKC 命令接收到偏移量的时间
    c->repl_ack_time = 0;
    // 客户端为从服务器时使用，记录了从服务器所使用的端口号
    c->slave_listening_port = 0;
    c->slave_ip[0] = '\0';
    c->slave_capa = SLAVE_CAPA_NONE;
    // 回复链表
    c->reply = listCreate();
    // 回复链表的字节量
    c->reply_bytes = 0;
    // 回复缓冲区大小达到软限制的时间
    c->obuf_soft_limit_reached_time = 0;
    // 回复链表的释放和复制函数
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    // 阻塞类型
    c->btype = BLOCKED_NONE;
    // 阻塞超时
    c->bpop.timeout = 0;
    // 造成客户端阻塞的列表键
    c->bpop.keys = dictCreate(&objectKeyPointerValueDictType,NULL);
    // 在解除阻塞时将元素推入到 target 指定的键中
    // BRPOPLPUSH 命令时使用
    c->bpop.target = NULL;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    // 进行事务时监视的键
    c->watched_keys = listCreate();
    // 订阅的频道和模式
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType,NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    // 如果不是伪客户端，那么添加到服务器的客户端链表中
    if (fd != -1) listAddNodeTail(server.clients,c);
    // 初始化客户端的事务状态
    initClientMultiState(c);
    // 返回客户端
    return c;
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * 这个函数在每次向客户端发送数据时都会被调用。函数的行为如下：
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * 当客户端可以接收新数据时（通常情况下都是这样），函数返回 REDIS_OK ，
 * 并将写处理器（write handler）安装到事件循环中，
 * 这样当套接字可写时，新数据就会被写入。
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * 对于那些不应该接收新数据的客户端，
 * 比如伪客户端、 master 以及 未 ONLINE 的 slave ，
 * 或者写处理器安装失败时，
 * 函数返回 REDIS_ERR 。
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 * 2) The client is a slave but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. 
 *
 * 通常在每个回复被创建时调用，如果函数返回 REDIS_ERR ，
 * 那么没有数据会被追加到输出缓冲区。
 */
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flags & (CLIENT_LUA|CLIENT_MODULE)) return C_OK;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    // 客户端是主服务器并且不接受查询，
    // 那么它是不可写的，出错
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    // 无连接的伪客户端总是不可写的
    if (c->fd <= 0) return C_ERR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket only
     * if not already done (there were no pending writes already and the client
     * was yet not flagged), and, for slaves, if the slave can actually
     * receive writes at this stage. */
    if (!clientHasPendingReplies(c) &&
        !(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack)))
    {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flags |= CLIENT_PENDING_WRITE;
        listAddNodeHead(server.clients_pending_write,c);
    }

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

/*
 * 尝试将回复添加到 c->buf 中
 */
int _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    // 正准备关闭客户端，无须再发送内容
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    // 回复链表里已经有内容，再添加内容到 c->buf 里面就是错误了
    if (listLength(c->reply) > 0) return C_ERR;

    /* Check that the buffer has enough space available for this string. */
    // 空间必须满足
    if (len > available) return C_ERR;

    // 复制内容到 c->buf 里面
    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return C_OK;
}

/*
 * 将回复对象（一个 SDS ）添加到 c->reply 回复链表中
 */
void _addReplyObjectToList(client *c, robj *o) {
    // 客户端即将被关闭，无须再发送回复
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    // 链表中无缓冲块，直接将对象追加到链表中
    if (listLength(c->reply) == 0) {
        sds s = sdsdup(o->ptr);
        listAddNodeTail(c->reply,s);
        // 链表中已有缓冲块，尝试将回复添加到块内
        // 如果当前的块不能容纳回复的话，那么新建一个块
        c->reply_bytes += sdslen(s);
    } else {
        listNode *ln = listLast(c->reply);
        // 取出表尾的 SDS
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        // 如果表尾 SDS 的已用空间加上对象的长度，小于 REDIS_REPLY_CHUNK_BYTES
        // 那么将新对象的内容拼接到表尾 SDS 的末尾
        if (tail && sdslen(tail)+sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatsds(tail,o->ptr);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(o->ptr);
        // 直接将对象追加到末尾
        } else {
            sds s = sdsdup(o->ptr);
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }
    // 检查回复缓冲区的大小，如果超过系统限制的话，那么关闭客户端
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
// 和 _addReplyObjectToList 类似，但会负责 SDS 的释放功能（如果需要的话）
void _addReplySdsToList(client *c, sds s) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+sdslen(s) <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatsds(tail,s);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(s);
            sdsfree(s);
        } else {
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

void _addReplyStringToList(client *c, const char *s, size_t len) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        // 为字符串创建字符串对象并追加到回复链表末尾
        sds node = sdsnewlen(s,len);
        listAddNodeTail(c->reply,node);
        c->reply_bytes += len;
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+len <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatlen(tail,s,len);
            listNodeValue(ln) = tail;
            c->reply_bytes += len;
        } else {
            sds node = sdsnewlen(s,len);
            listAddNodeTail(c->reply,node);
            c->reply_bytes += len;
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(client *c, robj *obj) {
    // 为客户端安装写处理器到事件循环
    if (prepareClientToWrite(c) != C_OK) return;

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * 如果在使用子进程，那么尽可能地避免修改对象的 refcount 域。
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. 
     *
     * 如果对象的编码为 RAW ，并且静态缓冲区中有空间
     * 那么就可以在不弄乱内存页的情况下，将对象发送给客户端。
     */
    if (sdsEncodedObject(obj)) {
        // 首先尝试复制内容到 c->buf 中，这样可以避免内存分配
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK)
            // 如果 c->buf 中的空间不够，就复制到 c->reply 链表中
            // 可能会引起内存分配
            _addReplyObjectToList(c,obj);
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* Optimization: if there is room in the static buffer for 32 bytes
         * (more than the max chars a 64 bit integer can take as string) we
         * avoid decoding the object and go for the lower level approach. */
        // 优化，如果 c->buf 中有等于或多于 32 个字节的空间
        // 那么将整数直接以字符串的形式复制到 c->buf 中
        if (listLength(c->reply) == 0 && (sizeof(c->buf) - c->bufpos) >= 32) {
            char buf[32];
            int len;

            len = ll2string(buf,sizeof(buf),(long)obj->ptr);
            if (_addReplyToBuffer(c,buf,len) == C_OK)
                return;
            /* else... continue with the normal code path, but should never
             * happen actually since we verified there is room. */
        }
        // 执行到这里，代表对象是整数，并且长度大于 32 位
        // 将它转换为字符串
        obj = getDecodedObject(obj);
        // 保存到缓存中
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/*
 * 将 SDS 中的内容复制到回复缓冲区
 */
void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == C_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Redis object
 * if not needed. The object will only be created by calling
 * _addReplyStringToList() if we fail to extend the existing tail object
 * in the list of objects. */
void addReplyString(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    if (_addReplyToBuffer(c,s,len) != C_OK)
        _addReplyStringToList(c,s,len);
}

void addReplyErrorLength(client *c, const char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

/*
 * 返回一个错误回复
 *
 * 例子 -ERR unknown command 'foobar'
 */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
}

void addReplyErrorFormat(client *c, const char *fmt, ...) {
    size_t l, j;
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted. */
    l = sdslen(s);
    for (j = 0; j < l; j++) {
        if (s[j] == '\r' || s[j] == '\n') s[j] = ' ';
    }
    addReplyErrorLength(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

/*
 * 返回一个状态回复
 *
 * 例子 +OK\r\n
 */
void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
// 当发送 Multi Bulk 回复时，先创建一个空的链表，之后再用实际的回复填充它
void *addDeferredMultiBulkLength(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (prepareClientToWrite(c) != C_OK) return NULL;
    listAddNodeTail(c->reply,NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

/* Populate the length object and try gluing it to the next chunk. */
// 设置 Multi Bulk 回复的长度
void setDeferredMultiBulkLength(client *c, void *node, long length) {
    listNode *ln = (listNode*)node;
    sds len, next;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addDeferredMultiBulkLength() */
    if (node == NULL) return;

    len = sdscatprintf(sdsnewlen("*",1),"%ld\r\n",length);
    listNodeValue(ln) = len;
    c->reply_bytes += sdslen(len);
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next != NULL) {
            len = sdscatsds(len,next);
            listDelNode(c->reply,ln->next);
            listNodeValue(ln) = len;
            /* No need to update c->reply_bytes: we are just moving the same
             * amount of bytes from one node to another. */
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* Add a double as a bulk reply */
/*
 * 以 bulk 回复的形式，返回一个双精度浮点数
 *
 * 例子 $4\r\n3.14\r\n
 */
void addReplyDouble(client *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        addReplyString(c,sbuf,slen);
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    robj *o = createStringObjectFromLongDouble(d,1);
    addReplyBulk(c,o);
    decrRefCount(o);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * 
 * 添加一个 long long 为整数回复，或者 bulk 或 multi bulk 的数目
 *
 * Basically this is used to output <prefix><long long><crlf>. 
 *
 * 输出格式为 <prefix><long long><crlf>
 *
 * 例子:
 *
 * *5\r\n10086\r\n
 *
 * $5\r\n10086\r\n
 */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        // 多条批量回复
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        // 批量回复
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

/*
 * 返回一个整数回复
 * 
 * 格式为 :10086\r\n
 */
void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

void addReplyMultiBulkLen(client *c, long length) {
    if (length < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.mbulkhdr[length]);
    else
        addReplyLongLongWithPrefix(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len;

    if (sdsEncodedObject(obj)) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }

    if (len < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.bulkhdr[len]);
    else
        addReplyLongLongWithPrefix(c,len,'$');
}

/* Add a Redis Object as a bulk reply 
 *
 * 返回一个 Redis 对象作为回复
 */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply 
 *
 * 返回一个 C 缓冲区作为回复
 */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s)  {
    addReplySds(c,sdscatfmt(sdsempty(),"$%u\r\n",
        (unsigned long)sdslen(s)));
    addReplySds(c,s);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply 
 *
 * 返回一个 long long 值作为回复
 */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
// 释放 dst 客户端原有的输出内容，并将 src 客户端的输出内容复制给 dst
void copyClientOutputBuffer(client *dst, client *src) {
    // 释放 dst 原有的回复链表
    listRelease(dst->reply);
    // 复制新链表到 dst
    dst->reply = listDup(src->reply);
    // 复制内容到回复 buf
    memcpy(dst->buf,src->buf,src->bufpos);
    // 同步偏移量和字节数
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    return c->bufpos || listLength(c->reply);
}

#define MAX_ACCEPTS_PER_CALL 1000
static void acceptCommonHandler(int fd, int flags, char *ip) {
    client *c;
    if ((c = createClient(fd)) == NULL) {
        serverLog(LL_WARNING,
            "Error registering fd event for the new client: %s (fd=%d)",
            strerror(errno),fd);
        close(fd); /* May be already closed, just ignore errors */
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in non-blocking
     * mode and we can send an error for free using the Kernel I/O */
    // 如果新添加的客户端令服务器的最大客户端数量达到了
    // 那么向新客户端写入错误信息，并关闭新客户端
    // 先创建客户端，再进行数量检查是为了方便地进行错误信息写入
    if (listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        freeClient(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (server.protected_mode &&
        server.bindaddr_count == 0 &&
        server.requirepass == NULL &&
        !(flags & CLIENT_UNIX_SOCKET) &&
        ip != NULL)
    {
        if (strcmp(ip,"127.0.0.1") && strcmp(ip,"::1")) {
            char *err =
                "-DENIED Redis is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (write(c->fd,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClient(c);
            return;
        }
    }
    // 更新连接次数
    server.stat_numconnections++;
    // 设置 FLAG
    c->flags |= flags;
}

/* 
 * 创建一个 TCP 连接处理器
 */
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        // accept 客户端连接
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        // 为客户端创建客户端状态（redisClient）
        acceptCommonHandler(cfd,0,cip);
    }
}

/*
 * 创建一个本地连接处理器
 */
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        // accept 本地客户端连接
        cfd = anetUnixAccept(server.neterr, fd);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted connection to %s", server.unixsocket);
        // 为本地客户端创建客户端状态
        acceptCommonHandler(cfd,CLIENT_UNIX_SOCKET,NULL);
    }
}

/*
 * 清空所有命令参数
 */
static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
// 断开所有从服务器的连接，强制所有从服务器执行重同步
void disconnectSlaves(void) {
    while (listLength(server.slaves)) {
        listNode *ln = listFirst(server.slaves);
        freeClient((client*)ln->value);
    }
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active socket.
     * If the client was already unlinked or if it's a "fake client" the
     * fd is already set to -1. */
    if (c->fd != -1) {
        /* Remove from the list of active clients. */
        ln = listSearchKey(server.clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients,ln);

        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = listSearchKey(server.clients_pending_write,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }
}

void freeClient(client *c) {
    listNode *ln;

    /* If it is our master that's beging disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (server.master && c->flags & CLIENT_MASTER) {
        serverLog(LL_WARNING,"Connection with master lost.");
        if (!(c->flags & (CLIENT_CLOSE_AFTER_REPLY|
                          CLIENT_CLOSE_ASAP|
                          CLIENT_BLOCKED|
                          CLIENT_UNBLOCKED)))
        {
            replicationCacheMaster(c);
            return;
        }
    }

    /* Log link disconnection with slave */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR)) {
        serverLog(LL_WARNING,"Connection with slave %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    sdsfree(c->pending_querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    if (c->flags & CLIENT_BLOCKED) unblockClient(c);
    dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    // 清空 WATCH 信息
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    // 退订所有频道和模式
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    /* Free data structures. */
    // 清空回复缓冲区
    listRelease(c->reply);
    // 清空命令参数
    freeClientArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    if (c->flags & CLIENT_SLAVE) {
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (c->flags & CLIENT_SLAVE && listLength(server.slaves) == 0)
            server.repl_no_slaves_since = server.unixtime;
        refreshGoodSlavesCount();
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    // 清除参数空间
    zfree(c->argv);
    // 清除事务状态信息
    freeClientMultiState(c);
    sdsfree(c->peerid);
    // 释放客户端 redisClient 结构本身
    zfree(c);
}

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
// 异步地释放给定的客户端
void freeClientAsync(client *c) {
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    listAddNodeTail(server.clients_to_close,c);
}

// 关闭需要异步关闭的客户端
void freeClientsInAsyncFreeQueue(void) {
    // 遍历所有要关闭的客户端
    while (listLength(server.clients_to_close)) {
        listNode *ln = listFirst(server.clients_to_close);
        client *c = listNodeValue(ln);

        c->flags &= ~CLIENT_CLOSE_ASAP;
        // 关闭客户端
        freeClient(c);
        // 从客户端链表中删除被关闭的客户端
        listDelNode(server.clients_to_close,ln);
    }
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed. */
int writeToClient(int fd, client *c, int handler_installed) {
    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    sds o;

    while(clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {
            // 写入内容到套接字
            // c->sentlen 是用来处理 short write 的
            // 当出现 short write ，导致写入未能一次完成时，
            // c->buf+c->sentlen 就会偏移到正确（未写入）内容的位置上。
            nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
            // 出错则跳出
            if (nwritten <= 0) break;
            // 成功写入则更新写入计数器变量
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            // 如果缓冲区中的内容已经全部写入完毕
            // 那么清空客户端的两个计数器变量
            if ((int)c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            // 取出位于链表最前面的对象
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o);

            // 略过空对象
            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            // 写入内容到套接字
            // c->sentlen 是用来处理 short write 的
            // 当出现 short write ，导致写入未能一次完成时，
            // c->buf+c->sentlen 就会偏移到正确（未写入）内容的位置上。
            nwritten = write(fd, o + c->sentlen, objlen - c->sentlen);
            // 写入出错则跳出
            if (nwritten <= 0) break;
            // 成功写入则更新写入计数器变量
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            // 如果缓冲区内容全部写入完毕，那么删除已写入完毕的节点
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
                c->reply_bytes -= objlen;
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * 为了避免一个非常大的回复独占服务器，
         * 当写入的总数量大于 REDIS_MAX_WRITE_PER_EVENT ，
         * 临时中断写入，将处理时间让给其他客户端，
         * 剩余的内容等下次写入就绪再继续写入
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver. 
         *
         * 不过，如果服务器的内存占用已经超过了限制，
         * 那么为了将回复缓冲区中的内容尽快写入给客户端，
         * 然后释放回复缓冲区的空间来回收内存，
         * 这时即使写入量超过了 REDIS_MAX_WRITE_PER_EVENT ，
         * 程序也继续进行写入
         */
        server.stat_net_output_bytes += totwritten;
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (server.maxmemory == 0 ||
             zmalloc_used_memory() < server.maxmemory)) break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (handler_installed) aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClient(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    writeToClient(fd,privdata,1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    int processed = listLength(server.clients_pending_write);

    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        listDelNode(server.clients_pending_write,ln);

        /* Try to write buffers to the client socket. */
        if (writeToClient(c->fd,c,0) == C_ERR) continue;

        /* If there is nothing left, do nothing. Otherwise install
         * the write handler. */
        if (clientHasPendingReplies(c) &&
            aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
                sendReplyToClient, c) == AE_ERR)
        {
            freeClientAsync(c);
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
/*
 * 处理内联命令，并创建参数对象
 *
 * 内联命令的各个参数以空格分开，并以 \r\n 结尾
 * 例子：
 *
 * <arg0> <arg1> <arg...> <argN>\r\n
 *
 * 这些内容会被用于创建参数对象，
 * 比如
 *
 * argv[0] = arg0
 * argv[1] = arg1
 * argv[2] = arg2
 */
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf,'\n');

    /* Nothing to do without a \r\n */
    // 收到的查询内容不符合协议格式，出错
    if (newline == NULL) {
        if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError("too big inline request",c,0);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    if (newline && newline != c->querybuf && *(newline-1) == '\r')
        newline--;

    /* Split the input buffer up to the \r\n */
    // 根据空格，分割命令的参数
    // 比如说 SET msg hello \r\n 将分割为
    // argv[0] = SET
    // argv[1] = msg
    // argv[2] = hello
    // argc = 3
    querylen = newline-(c->querybuf);
    aux = sdsnewlen(c->querybuf,querylen);
    argv = sdssplitargs(aux,&argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request",c,0);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && c->flags & CLIENT_SLAVE)
        c->repl_ack_time = server.unixtime;

    /* Leave data after the first line of the query in the buffer */
    // 从缓冲区中删除已 argv 已读取的内容
    // 剩余的内容是未读取的
    sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*argc);
    }

    /* Create redis objects for all arguments. */
    // 为每个参数创建一个字符串对象
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            // argv[j] 已经是 SDS 了
            // 所以创建的字符串对象直接指向该 SDS
            c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return C_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c, int pos) {
    if (server.verbosity <= LL_VERBOSE) {
        sds client = catClientInfoString(sdsempty(),c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf) < PROTO_DUMP_LEN) {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%s'", c->querybuf);
        } else {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'", PROTO_DUMP_LEN/2, c->querybuf, sdslen(c->querybuf)-PROTO_DUMP_LEN, PROTO_DUMP_LEN/2, c->querybuf+sdslen(c->querybuf)-PROTO_DUMP_LEN/2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        serverLog(LL_VERBOSE,
            "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    sdsrange(c->querybuf,pos,-1);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
/*
 * 将 c->querybuf 中的协议内容转换成 c->argv 中的参数对象
 * 
 * 比如 *3\r\n$3\r\nSET\r\n$3\r\nMSG\r\n$5\r\nHELLO\r\n
 * 将被转换为：
 * argv[0] = SET
 * argv[1] = MSG
 * argv[2] = HELLO
 */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    // 读入命令的参数个数
    // 比如 *3\r\n$3\r\nSET\r\n... 将令 c->multibulklen = 3
    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        // 检查缓冲区的内容第一个 "\r\n"
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string",c,0);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        // 协议的第一个字符必须是 '*'
        serverAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        // 将参数个数，也即是 * 之后， \r\n 之前的数字取出并保存到 ll 中
        // 比如对于 *3\r\n ，那么 ll 将等于 3
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        // 参数的数量超出限制
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count",c,pos);
            return C_ERR;
        }

        // 参数数量之后的位置
        // 比如对于 *3\r\n$3\r\n$SET\r\n... 来说，
        // pos 指向 *3\r\n$3\r\n$SET\r\n...
        //                ^
        //                |
        //               pos
        pos = (newline-c->querybuf)+2;
        // 如果 ll <= 0 ，那么这个命令是一个空白命令
        // 那么将这段内容从查询缓冲区中删除，只保留未阅读的那部分内容
        // 为什么参数可以是空的呢？
        // processInputBuffer 中有注释到 "Multibulk processing could see a <= 0 length"
        // 但并没有详细说明原因
        if (ll <= 0) {
            sdsrange(c->querybuf,pos,-1);
            return C_OK;
        }

        // 设置参数数量
        c->multibulklen = ll;

        /* Setup argv array on client structure */
        // 根据参数数量，为各个参数对象分配空间
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    // 从 c->querybuf 中读入参数，并创建各个参数对象到 c->argv
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        // 读入参数长度
        if (c->bulklen == -1) {
            // 确保 "\r\n" 存在
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string",c,0);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            // 确保协议符合参数格式，检查其中的 $...
            // 比如 $3\r\nSET\r\n
            if (c->querybuf[pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError("expected $ but got something else",c,pos);
                return C_ERR;
            }

            // 读取长度
            // 比如 $3\r\nSET\r\n 将会让 ll 的值设置 3
            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length",c,pos);
                return C_ERR;
            }

            // 定位到参数的开头
            // 比如 
            // $3\r\nSET\r\n...
            //       ^
            //       |
            //      pos
            pos += newline-(c->querybuf+pos)+2;
            // 如果参数非常长，那么做一些预备措施来优化接下来的参数复制操作
            if (ll >= PROTO_MBULK_BIG_ARG) {
                size_t qblen;

                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data. */
                sdsrange(c->querybuf,pos,-1);
                pos = 0;
                qblen = sdslen(c->querybuf);
                /* Hint the sds library about the amount of bytes this string is
                 * going to contain. */
                if (qblen < (size_t)ll+2)
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-qblen);
            }
            // 参数的长度
            c->bulklen = ll;
        }

        /* Read bulk argument */
        // 读入参数
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            // 确保内容符合协议格式
            // 比如 $3\r\nSET\r\n 就检查 SET 之后的 \r\n
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            // 为参数创建字符串对象  
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                (signed) sdslen(c->querybuf) == c->bulklen+2)
            {
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(NULL,c->bulklen+2);
                sdsclear(c->querybuf);
                pos = 0;
            } else {
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+pos,c->bulklen);
                pos += c->bulklen+2;
            }
            // 清空参数长度
            c->bulklen = -1;
            // 减少还需读入的参数个数
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    // 从 querybuf 中删除已被读取的内容
    if (pos) sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    // 如果本条命令的所有参数都已读取完，那么返回
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    // 如果还有参数未读取完，那么就协议内容有错
    return C_ERR;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */
void processInputBuffer(client *c) {
    server.current_client = c;
    /* Keep processing while there is something in the input buffer */
    // 尽可能地处理查询缓冲区中的内容
    // 如果读取出现 short read ，那么可能会有内容滞留在读取缓冲区里面
    // 这些滞留内容也许不能完整构成一个符合协议的命令，
    // 需要等待下次读事件的就绪
    while(sdslen(c->querybuf)) {
        /* Return if clients are paused. */
        // 如果客户端正处于暂停状态，那么直接返回
        if (!(c->flags & CLIENT_SLAVE) && clientsArePaused()) break;

        /* Immediately abort if the client is in the middle of something. */
        // REDIS_BLOCKED 状态表示客户端正在被阻塞
        if (c->flags & CLIENT_BLOCKED) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        // 判断请求的类型
        // 两种类型的区别可以在 Redis 的通讯协议上查到：
        // http://redis.readthedocs.org/en/latest/topic/protocol.html
        // 简单来说，多条查询是一般客户端发送来的，
        // 而内联查询则是 TELNET 发送来的
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                // 多条查询
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                // 内联查询
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        // 将缓冲区中的内容转换成命令，以及命令参数
        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != C_OK) break;
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            // 执行命令，并重置客户端
            if (processCommand(c) == C_OK) {
                if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
                    /* Update the applied replication offset of our master. */
                    c->reploff = c->read_reploff - sdslen(c->querybuf);
                }
                resetClient(c);
            }
            /* freeMemoryIfNeeded may flush slave output buffers. This may result
             * into a slave, that may be the active client, to be freed. */
            if (server.current_client == NULL) break;
        }
    }
    server.current_client = NULL;
}

/*
 * 读取客户端的查询缓冲区内容
 */
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client*) privdata;
    int nread, readlen;
    size_t qblen;
    UNUSED(el);
    UNUSED(mask);

    // 设置服务器的当前客户端
    // 读入长度（默认为 16 MB）
    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        int remaining = (unsigned)(c->bulklen+2)-sdslen(c->querybuf);

        if (remaining < readlen) readlen = remaining;
    }

    // 获取查询缓冲区当前内容的长度
    // 如果读取出现 short read ，那么可能会有内容滞留在读取缓冲区里面
    // 这些滞留内容也许不能完整构成一个符合协议的命令，
    qblen = sdslen(c->querybuf);
    // 如果有需要，更新缓冲区内容长度的峰值（peak）
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    // 为查询缓冲区分配空间
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
    // 读入内容到查询缓存
    nread = read(fd, c->querybuf+qblen, readlen);
    // 读入出错
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    // 遇到 EOF
    } else if (nread == 0) {
        serverLog(LL_VERBOSE, "Client closed connection");
        freeClient(c);
        return;
    } else if (c->flags & CLIENT_MASTER) {
        /* Append the query buffer to the pending (not applied) buffer
         * of the master. We'll use this buffer later in order to have a
         * copy of the string applied by the last command executed. */
        c->pending_querybuf = sdscatlen(c->pending_querybuf,
                                        c->querybuf+qblen,nread);
    }

        // 根据内容，更新查询缓冲区（SDS） free 和 len 属性
        // 并将 '\0' 正确地放到内容的最后
    sdsIncrLen(c->querybuf,nread);
        // 记录服务器和客户端最后一次互动的时间
    c->lastinteraction = server.unixtime;
        // 如果客户端是 master 的话，更新它的复制偏移量
    if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    server.stat_net_input_bytes += nread;
    // 查询缓冲区长度超出服务器最大缓冲区长度
    // 清空缓冲区并释放客户端
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClient(c);
        return;
    }

    /* Time to process the buffer. If the client is a master we need to
     * compute the difference between the applied offset before and after
     * processing the buffer, to understand how much of the replication stream
     * was actually applied to the master state: this quantity, and its
     * corresponding part of the replication stream, will be propagated to
     * the sub-slaves and to the replication backlog. */
    if (!(c->flags & CLIENT_MASTER)) {
        processInputBuffer(c);
    } else {
        size_t prev_offset = c->reploff;
        processInputBuffer(c);
        size_t applied = c->reploff - prev_offset;
        if (applied) {
            replicationFeedSlavesFromMasterStream(server.slaves,
                    c->pending_querybuf, applied);
            sdsrange(c->pending_querybuf,applied,-1);
        }
    }
}
// 获取客户端目前最大的一块缓冲区的大小
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Peer ID" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * A Peer ID always fits inside a buffer of NET_PEER_ID_LEN bytes, including
 * the null term.
 *
 * The function returns REDIS_OK on succcess, and REDIS_ERR on failure.
 *
 * On failure the function still populates 'peerid' with the "?:0" string
 * in case you want to relax error checking or need to display something
 * anyway (see anetPeerToString implementation for more info). */
void genClientPeerId(client *client, char *peerid,
                            size_t peerid_len) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(peerid,peerid_len,"%s:0",server.unixsocket);
    } else {
        /* TCP client. */
        anetFormatPeer(client->fd,peerid,peerid_len);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_PEER_ID_LEN];

    if (c->peerid == NULL) {
        genClientPeerId(c,peerid,sizeof(peerid));
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* Concatenate a string representing the state of a client in an human
 * readable format, into the sds string 's'. */
// 获取客户端的各项信息，将它们储存到 sds 值 s 里面，并返回。
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], *p;
    int emask;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    emask = client->fd == -1 ? 0 : aeGetFileEvents(server.el,client->fd);
    p = events;
    if (emask & AE_READABLE) *p++ = 'r';
    if (emask & AE_WRITABLE) *p++ = 'w';
    *p = '\0';
    return sdscatfmt(s,
        "id=%U addr=%s fd=%i name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U obl=%U oll=%U omem=%U events=%s cmd=%s",
        (unsigned long long) client->id,
        getClientPeerId(client),
        client->fd,
        client->name ? (char*)client->name->ptr : "",
        (long long)(server.unixtime - client->ctime),
        (long long)(server.unixtime - client->lastinteraction),
        flags,
        client->db->id,
        (int) dictSize(client->pubsub_channels),
        (int) listLength(client->pubsub_patterns),
        (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        (unsigned long long) sdslen(client->querybuf),
        (unsigned long long) sdsavail(client->querybuf),
        (unsigned long long) client->bufpos,
        (unsigned long long) listLength(client->reply),
        (unsigned long long) getClientOutputBufferMemoryUsage(client),
        events,
        client->lastcmd ? client->lastcmd->name : "NULL");
}

/*
 * 打印出所有连接到服务器的客户端的信息
 */
sds getAllClientsInfoString(void) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(NULL,200*listLength(server.clients));
    sdsclear(o);
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

/*
 * CLIENT 命令的实现
 */
void clientCommand(client *c) {
    listNode *ln;
    listIter li;
    client *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        /* CLIENT LIST */
        sds o = getAllClientsInfoString();
        addReplyBulkCBuffer(c,o,sdslen(o));
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        int type = -1;
        uint64_t id = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while(i < c->argc) {
                int moreargs = c->argc > i+1;

                if (!strcasecmp(c->argv[i]->ptr,"id") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c,c->argv[i+1],&tmp,NULL)
                        != C_OK) return;
                    id = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr,"type") && moreargs) {
                    type = getClientTypeByName(c->argv[i+1]->ptr);
                    if (type == -1) {
                        addReplyErrorFormat(c,"Unknown client type '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"addr") && moreargs) {
                    addr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"skipme") && moreargs) {
                    if (!strcasecmp(c->argv[i+1]->ptr,"yes")) {
                        skipme = 1;
                    } else if (!strcasecmp(c->argv[i+1]->ptr,"no")) {
                        skipme = 0;
                    } else {
                        addReply(c,shared.syntaxerr);
                        return;
                    }
                } else {
                    addReply(c,shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client = listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client),addr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (c == client && skipme) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                freeClient(client);
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp(c->argv[1]->ptr,"setname") && c->argc == 3) {
        int j, len = sdslen(c->argv[2]->ptr);
        char *p = c->argv[2]->ptr;

        /* Setting the client name to an empty string actually removes
         * the current name. */
        // 名字为空时，清空客户端的名字
        if (len == 0) {
            if (c->name) decrRefCount(c->name);
            c->name = NULL;
            addReply(c,shared.ok);
            return;
        }

        /* Otherwise check if the charset is ok. We need to do this otherwise
         * CLIENT LIST format will break. You should always be able to
         * split by space to get the different fields. */
        for (j = 0; j < len; j++) {
            if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
                addReplyError(c,
                    "Client names cannot contain spaces, "
                    "newlines or special characters.");
                return;
            }
        }
        if (c->name) decrRefCount(c->name);
        c->name = c->argv[2];
        incrRefCount(c->name);
        addReply(c,shared.ok);
    // CLIENT getname 获取客户端的名字
    } else if (!strcasecmp(c->argv[1]->ptr,"getname") && c->argc == 2) {
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReply(c,shared.nullbulk);
    } else if (!strcasecmp(c->argv[1]->ptr,"pause") && c->argc == 3) {
        long long duration;

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&duration,UNIT_MILLISECONDS)
                                        != C_OK) return;
        pauseClients(duration);
        addReply(c,shared.ok);
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME | PAUSE | REPLY)");
    }
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time;
    time_t now = time(NULL);

    if (labs(now-logged_time) > 60) {
        serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection aborted.");
        logged_time = now;
    }
    freeClientAsync(c);
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
// 修改客户端的参数数组
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    // 创建新参数
    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    // 释放旧参数
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    // 用新参数替换
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    freeClientArgv(c);
    zfree(c->argv);
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;

    if (i >= c->argc) {
        c->argv = zrealloc(c->argv,sizeof(robj*)*(i+1));
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    c->argv[i] = newval;
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is virtually
 * using to store the reply still not read by the client.
 * It is "virtual" since the reply output list may contain objects that
 * are shared and are not really using additional memory.
 *
 * 函数返回客用于保存目前仍未返回给客户端的回复的虚拟大小（以字节为单位）。
 * 之所以说是虚拟大小，因为回复列表中可能有包含共享的对象。
 *
 * The function returns the total sum of the length of all the objects
 * stored in the output list, plus the memory used to allocate every
 * list node. The static reply buffer is not taken into account since it
 * is allocated anyway.
 *
 * 函数返回回复列表中所包含的全部对象的体积总和，
 * 加上列表节点所分配的空间。
 * 静态回复缓冲区不会被计算在内，因为它总是会被分配的。
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. 
 *
 * 注意：这个函数的速度很快，所以它可以被随意地调用多次。
 * 这个函数目前的主要作用就是用来强制客户端输出长度限制。
 */
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(listNode)+5;
    /* The +5 above means we assume an sds16 hdr, may not be true
     * but is not going to be a problem. */

    return c->reply_bytes + (list_item_size*listLength(c->reply));
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * 获取客户端的类型，用于对不同类型的客户端应用不同的限制。
 *
 * The function will return one of the following:
 * 
 * 函数将返回以下三个值的其中一个：
 *
 * CLIENT_TYPE_NORMAL -> Normal client
 *                                    普通客户端
 *
 * CLIENT_TYPE_SLAVE  -> Slave or client executing MONITOR command
 *                                    从服务器，或者正在执行 MONITOR 命令的客户端
 *
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 *                                    正在进行订阅操作（SUBSCRIBE/PSUBSCRIBE）的客户端
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

// 根据名字，获取客户端的类型常量
int getClientTypeByName(char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

// 根据客户端的类型，获取名字
char *getClientTypeName(int class) {
    switch(class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * 这个函数检查客户端是否达到了输出缓冲区的软性（soft）限制或者硬性（hard）限制，
 * 并在到达软限制时，对客户端进行标记。
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. 
 *
 * 返回值：到达软性限制或者硬性限制时，返回非 0 值。
 *         否则返回 0 。
 */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    // 获取客户端回复缓冲区的大小
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    // 获取客户端的限制大小
    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;
    // 检查软性限制
    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].hard_limit_bytes)
        hard = 1;
    // 检查硬性限制
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    // 达到软性限制
    if (soft) {
        // 第一次达到软性限制
        if (c->obuf_soft_limit_reached_time == 0) {
            // 记录时间
            c->obuf_soft_limit_reached_time = server.unixtime;
            // 关闭软性限制 flag
            soft = 0; /* First time we see the soft limit reached */
        // 再次达到软性限制
        } else {
            // 软性限制的连续时长
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            // 如果没有超过最大连续时长的话，那么关闭软性限制 flag
            // 如果超过了最大连续时长的话，软性限制 flag 就会被保留
            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        // 未达到软性限制，或者已脱离软性限制，那么清空软性限制的进入时间
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * 如果客户端达到缓冲区大小的软性或者硬性限制，那么打开客户端的 ``REDIS_CLOSE_ASAP`` 状态，
 * 让服务器异步地关闭客户端。
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers. 
 *
 * 注意：
 * 我们不能直接关闭客户端，而要异步关闭的原因是客户端正处于一个不能被安全地关闭的上下文中。
 * 比如说，可能有底层函数正在推入数据到客户端的输出缓冲区里面。      
 */
void asyncCloseClientOnOutputBufferLimitReached(client *c) {
    serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
    // 已经被标记了
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return;
    // 检查限制
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(),c);

        // 异步关闭
        freeClientAsync(c);
        serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
    }
}

/* Helper function used by freeMemoryIfNeeded() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        int events;

        /* Note that the following will not flush output buffers of slaves
         * in STATE_ONLINE but having put_online_on_ack set to true: in this
         * case the writable event is never installed, since the purpose
         * of put_online_on_ack is to postpone the moment it is installed.
         * This is what we want since slaves in this state should not receive
         * writes before the first ACK. */
        events = aeGetFileEvents(server.el,slave->fd);
        if (events & AE_WRITABLE &&
            slave->replstate == SLAVE_STATE_ONLINE &&
            clientHasPendingReplies(slave))
        {
            writeToClient(slave->fd,slave,0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms). While clients
 * are paused no command is processed from clients, so the data set can't
 * change during that time.
 *
 * However while this function pauses normal and Pub/Sub clients, slaves are
 * still served, so this function can be used on server upgrades where it is
 * required that slaves process the latest bytes from the replication stream
 * before being turned to masters.
 *
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the pause is extended if the duration is more than the
 * time left for the previous duration. However if the duration is smaller
 * than the time left for the previous pause, no change is made to the
 * left duration. */
// 暂停客户端，让服务器在指定的时间内不再接受被暂停客户端发来的命令
// 可以用于系统更新，并在内部由 CLUSTER FAILOVER 命令使用。
void pauseClients(mstime_t end) {
    // 设置暂停时间
    if (!server.clients_paused || end > server.clients_pause_end_time)
        server.clients_pause_end_time = end;
    // 打开客户端的“已被暂停”标志
    server.clients_paused = 1;
}

/* Return non-zero if clients are currently paused. As a side effect the
 * function checks if the pause time was reached and clear it. */
 // 判断服务器目前被暂停客户端的数量，没有任何客户端被暂停时，返回 0 。
int clientsArePaused(void) {
    if (server.clients_paused &&
        server.clients_pause_end_time < server.mstime)
    {
        listNode *ln;
        listIter li;
        client *c;

        server.clients_paused = 0;

        /* Put all the clients in the unblocked clients queue in order to
         * force the re-processing of the input buffer if any. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            c = listNodeValue(ln);

            /* Don't touch slaves and blocked clients. The latter pending
             * requests be processed when unblocked. */
            if (c->flags & (CLIENT_SLAVE|CLIENT_BLOCKED)) continue;
            c->flags |= CLIENT_UNBLOCKED;
            listAddNodeTail(server.unblocked_clients,c);
        }
    }
    return server.clients_paused;
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
// 让服务器在被阻塞的情况下，仍然处理某些事件。
int processEventsWhileBlocked(void) {
    int iterations = 4; /* See the function top-comment. */
    int count = 0;
    while (iterations--) {
        int events = 0;
        events += aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        events += handleClientsWithPendingWrites();
        if (!events) break;
        count += events;
    }
    return count;
}
