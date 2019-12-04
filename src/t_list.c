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

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * 将给定元素添加到列表的表头或表尾。
 *
 * 参数 where 决定了新元素添加的位置：
 *
 *  - REDIS_HEAD 将新元素添加到表头
 *
 *  - REDIS_TAIL 将新元素添加到表尾
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. 
 *
 * 调用者无须担心 value 的引用计数，因为这个函数会负责这方面的工作。
 */
void listTypePush(robj *subject, robj *value, int where) {
    // ZIPLIST
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        // 取出对象的值，因为 ZIPLIST 只能保存字符串或整数
        value = getDecodedObject(value);
        size_t len = sdslen(value->ptr);
        quicklistPush(subject->ptr, value->ptr, len, pos);
        decrRefCount(value);
    // 双端链表
    } else {
        serverPanic("Unknown list encoding");
    }
}

void *listPopSaver(unsigned char *data, unsigned int sz) {
    return createStringObject((char*)data,sz);
}

/*
 * 从列表的表头或表尾中弹出一个元素。
 *
 * 参数 where 决定了弹出元素的位置： 
 *
 *  - REDIS_HEAD 从表头弹出
 *
 *  - REDIS_TAIL 从表尾弹出
 */
robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

/*
 * 返回列表的节点数量
 */
unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index.
 *
 * 创建并返回一个列表迭代器。
 *
 * 参数 index 决定开始迭代的列表索引。
 *
 * 参数 direction 则决定了迭代的方向。
 *
 * listTypeIterator 于 redis.h 文件中定义。
 */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards tail. */
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. 
 *
 * 释放迭代器
 */
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li->iter);
    zfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. 
 *
 * 使用 entry 结构记录迭代器当前指向的节点，并将迭代器的指针移动到下一个元素。
 *
 * 如果列表中还有元素可迭代，那么返回 1 ，否则，返回 0 。
 */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    serverAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistNext(li->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
    // 列表元素已经全部迭代完
    return 0;
}

/* Return entry or NULL at the current position of the iterator. 
 *
 * 返回 entry 结构当前所保存的列表节点。
 *
 * 如果 entry 没有记录任何节点，那么返回 NULL 。
 */
robj *listTypeGet(listTypeEntry *entry) {
    robj *value = NULL;
    // 根据索引，从 ZIPLIST 中取出节点的值
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (entry->entry.value) {
            value = createStringObject((char *)entry->entry.value,
                                       entry->entry.sz);
        } else {
            value = createStringObjectFromLongLong(entry->entry.longval);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

/*
 * 将对象 value 插入到列表节点的之前或之后。
 *
 * where 参数决定了插入的位置：
 *
 *  - REDIS_HEAD 插入到节点之前
 *
 *  - REDIS_TAIL 插入到节点之后
 */
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    // 插入到 ZIPLIST
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        // 返回对象未编码的值
        value = getDecodedObject(value);
        sds str = value->ptr;
        size_t len = sdslen(str);
        if (where == LIST_TAIL) {
            quicklistInsertAfter((quicklist *)entry->entry.quicklist,
                                 &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore((quicklist *)entry->entry.quicklist,
                                  &entry->entry, str, len);
        }
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. 
 *
 * 将当前节点的值和对象 o 进行对比
 *
 * 函数在两值相等时返回 1 ，不相等时返回 0 。
 */
int listTypeEqual(listTypeEntry *entry, robj *o) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        serverAssertWithInfo(NULL,o,sdsEncodedObject(o));
        return quicklistCompare(entry->entry.zi,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. 
 *
 * 删除 entry 所指向的节点
 */
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/*
 * 将列表的底层编码从 ziplist 转换成双端链表
 */
void listTypeConvert(robj *subject, int enc) {
    serverAssertWithInfo(NULL,subject,subject->type==OBJ_LIST);
    serverAssertWithInfo(NULL,subject,subject->encoding==OBJ_ENCODING_ZIPLIST);
    // 转换成双端链表
    if (enc == OBJ_ENCODING_QUICKLIST) {
        size_t zlen = server.list_max_ziplist_size;
        int depth = server.list_compress_depth;
        subject->ptr = quicklistCreateFromZiplist(zlen, depth, subject->ptr);
        // 更新编码
        subject->encoding = OBJ_ENCODING_QUICKLIST;
    } else {
        serverPanic("Unsupported list conversion");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

void pushGenericCommand(client *c, int where) {
    int j, pushed = 0;
    // 取出列表对象
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);

    if (lobj && lobj->type != OBJ_LIST) {
        addReply(c,shared.wrongtypeerr);
        return;
    }

    // 遍历所有输入值，并将它们添加到列表中
    for (j = 2; j < c->argc; j++) {
        // 如果列表对象不存在，那么创建一个，并关联到数据库
        if (!lobj) {
            lobj = createQuicklistObject();
            quicklistSetOptions(lobj->ptr, server.list_max_ziplist_size,
                                server.list_compress_depth);
            dbAdd(c->db,c->argv[1],lobj);
        }
        // 将值推入到列表
        listTypePush(lobj,c->argv[j],where);
        pushed++;
    }
    // 返回添加的节点数量
    addReplyLongLong(c, (lobj ? listTypeLength(lobj) : 0));
    // 如果至少有一个元素被成功推入，那么执行以下代码
    if (pushed) {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";

        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    server.dirty += pushed;
}

void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD);
}

void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL);
}

void pushxGenericCommand(client *c, int where) {
    int j, pushed = 0;
    robj *subject;

    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    for (j = 2; j < c->argc; j++) {
        listTypePush(subject,c->argv[j],where);
        pushed++;
    }

    addReplyLongLong(c,listTypeLength(subject));

    if (pushed) {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    server.dirty += pushed;
}

void lpushxCommand(client *c) {
    pushxGenericCommand(c,LIST_HEAD);
}

void rpushxCommand(client *c) {
    pushxGenericCommand(c,LIST_TAIL);
}

void linsertCommand(client *c) {
    int where;
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        where = LIST_TAIL;
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        where = LIST_HEAD;
    } else {
        addReply(c,shared.syntaxerr);
        return;
    }

    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    /* Seek pivot from head to tail */
    iter = listTypeInitIterator(subject,0,LIST_TAIL);
    while (listTypeNext(iter,&entry)) {
        if (listTypeEqual(&entry,c->argv[3])) {
            listTypeInsert(&entry,c->argv[4],where);
            inserted = 1;
            break;
        }
    }
    listTypeReleaseIterator(iter);

    if (inserted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                            c->argv[1],c->db->id);
        server.dirty++;
    } else {
        /* Notify client of a failed insert */
        addReply(c,shared.cnegone);
        return;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

void llenCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    addReplyLongLong(c,listTypeLength(o));
}

void lindexCommand(client *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    robj *value = NULL;

    // 取出整数值对象 index
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    // 根据索引，遍历 ziplist ，直到指定位置
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;
        if (quicklistIndex(o->ptr, index, &entry)) {
            if (entry.value) {
                value = createStringObject((char*)entry.value,entry.sz);
            } else {
                value = createStringObjectFromLongLong(entry.longval);
            }
            addReplyBulk(c,value);
            decrRefCount(value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

void lsetCommand(client *c) {
    // 取出列表对象
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    robj *value = c->argv[3];

    // 取出整数值对象 index
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               value->ptr, sdslen(value->ptr));
        if (!replaced) {
            addReply(c,shared.outofrangeerr);
        } else {
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

void popGenericCommand(client *c, int where) {
    // 取出列表对象
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;

    // 弹出列表元素
    robj *value = listTypePop(o,where);
    // 根据弹出元素是否为空，决定后续动作
    if (value == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

        addReplyBulk(c,value);
        decrRefCount(value);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
        if (listTypeLength(o) == 0) {
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                c->argv[1],c->db->id);
            dbDelete(c->db,c->argv[1]);
        }
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

void lrangeCommand(client *c) {
    robj *o;
    long start, end, llen, rangelen;

    // 取出索引值 start 和 end
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    // 取出列表对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
         || checkType(c,o,OBJ_LIST)) return;
    // 取出列表长度
    llen = listTypeLength(o);

    /* convert negative indexes */
    // 将负数索引转换成正数索引
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        listTypeIterator *iter = listTypeInitIterator(o, start, LIST_TAIL);

        while(rangelen--) {
            listTypeEntry entry;
            listTypeNext(iter, &entry);
            quicklistEntry *qe = &entry.entry;
            if (qe->value) {
                addReplyBulkCBuffer(c,qe->value,qe->sz);
            } else {
                addReplyBulkLongLong(c,qe->longval);
            }
        }
        listTypeReleaseIterator(iter);
    } else {
        serverPanic("List encoding is not QUICKLIST!");
    }
}

void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;

    // 取出索引值 start 和 end
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    // 取出列表对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,OBJ_LIST)) return;
    // 列表长度
    llen = listTypeLength(o);

    /* convert negative indexes */
    // 将负数索引转换成正数索引
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    // 删除指定列表两端的元素
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        // 删除左端元素
        quicklistDelRange(o->ptr,0,ltrim);
        // 删除右端元素
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }

    // 发送通知
    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    // 如果列表已经为空，那么删除它
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
    addReply(c,shared.ok);
}

void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;

    // 取出指定删除模式的 count 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != C_OK))
        return;

    // 取出列表对象
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) return;

    listTypeIterator *li;
    // 根据 toremove 参数，决定是从表头还是表尾开始进行删除
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;
    // 查找，比对对象，并进行删除
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;
            // 已经满足删除数量，停止
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    if (removed) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"lrem",c->argv[1],c->db->id);
    }

    if (listTypeLength(subject) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    addReplyLongLong(c,removed);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

void rpoplpushHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value) {
    /* Create the list if the key does not exist */
    // 如果目标列表不存在，那么创建一个
    if (!dstobj) {
        dstobj = createQuicklistObject();
        quicklistSetOptions(dstobj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        dbAdd(c->db,dstkey,dstobj);
    }
    signalModifiedKey(c->db,dstkey);
    // 将值推入目标列表中
    listTypePush(dstobj,value,LIST_HEAD);
    notifyKeyspaceEvent(NOTIFY_LIST,"lpush",dstkey,c->db->id);
    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

void rpoplpushCommand(client *c) {
    robj *sobj, *value;
    // 来源列表
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,sobj,OBJ_LIST)) return;

    // 空列表，没有元素可 pop ，直接返回
    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        addReply(c,shared.nullbulk);
    // 源列表非空
    } else {
        // 目标对象
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        // 检查目标对象是否列表
        if (dobj && checkType(c,dobj,OBJ_LIST)) return;
        // 从源列表中弹出值
        value = listTypePop(sobj,LIST_TAIL);
        /* We saved touched key, and protect it, since rpoplpushHandlePush
         * may change the client command argument vector (it does not
         * currently). */
        incrRefCount(touchedkey);
        // 将值推入目标列表中，如果目标列表不存在，那么创建一个新列表
        rpoplpushHandlePush(c,c->argv[2],dobj,value);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        notifyKeyspaceEvent(NOTIFY_LIST,"rpop",touchedkey,c->db->id);
        // 如果源列表已经为空，那么将它删除
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        signalModifiedKey(c->db,touchedkey);
        decrRefCount(touchedkey);
        server.dirty++;
    }
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* This is how the current blocking POP works, we use BLPOP as example:
 *
 * 以下是目前的阻塞 POP 操作的运作方法，以 BLPOP 作为例子：
 *
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 *
 * - 如果用户调用 BLPOP ，并且列表非空，那么程序执行 LPOP 。
 *   因此，当列表非空时，调用 BLPOP 等于调用 LPOP。
 *
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 *
 * - 当 BLPOP 对一个空键执行时，客户端才会被阻塞：
 *   服务器不再对这个客户端发送任何数据，
 *   对这个客户端的状态设为“被阻塞“，直到解除阻塞为止。
 *   并且客户端会被加入到一个以阻塞键为 key ，
 *   以被阻塞客户端为 value 的字典 db->blocking_keys 中。
 *
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 *
 * - 当有 PUSH 命令作用于一个造成客户端阻塞的键时，
 *   程序将这个键标记为“就绪”，并且在执行完这个命令、事务、或脚本之后，
 *   程序会按“先阻塞先服务”的顺序，将列表的元素返回给那些被阻塞的客户端，
 *   被解除阻塞的客户端数量取决于 PUSH 命令推入的元素数量。
 */

/* Set a client in blocking mode for the specified key, with the specified
 * timeout */
// 根据给定数量的 key ，对给定客户端进行阻塞
// 参数：
// keys    任意多个 key
// numkeys keys 的键数量
// timeout 阻塞的最长时限
// target  在解除阻塞时，将结果保存到这个 key 对象，而不是返回给客户端
//         只用于 BRPOPLPUSH 命令
void blockForKeys(client *c, robj **keys, int numkeys, mstime_t timeout, robj *target) {
    dictEntry *de;
    list *l;
    int j;

    // 设置阻塞状态的超时和目标选项
    c->bpop.timeout = timeout;
    // target 在执行 RPOPLPUSH 命令时使用
    c->bpop.target = target;

    if (target != NULL) incrRefCount(target);

    // 关联阻塞客户端和键的相关信息
    for (j = 0; j < numkeys; j++) {
        /* If the key already exists in the dict ignore it. */
        // c->bpop.keys 是一个集合（值为 NULL 的字典）
        // 它记录所有造成客户端阻塞的键
        // 以下语句在键不存在于集合的时候，将它添加到集合
        if (dictAdd(c->bpop.keys,keys[j],NULL) != DICT_OK) continue;
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        // c->db->blocking_keys 字典的键为造成客户端阻塞的键
        // 而值则是一个链表，链表中包含了所有被阻塞的客户端
        // 以下程序将阻塞键和被阻塞客户端关联起来
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            // 链表不存在，新创建一个，并将它关联到字典中
            int retval;

            /* For every key we take a list of clients blocked for it */
            l = listCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            serverAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            l = dictGetVal(de);
        }
        // 将客户端填接到被阻塞客户端的链表中
        listAddNodeTail(l,c);
    }
    blockClient(c,BLOCKED_LIST);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;
    list *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    // 遍历所有 key ，将它们从客户端 db->blocking_keys 的链表中移除
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);

        /* Remove this client from the list of clients waiting for this key. */
        // 获取所有因为 key 而被阻塞的客户端的链表
        l = dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        // 将指定客户端从链表中删除
        listDelNode(l,listSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        // 如果已经没有其他客户端阻塞在这个 key 上，那么删除这个链表
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    // 清空 bpop.keys 集合（字典）
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * 如果有客户端正因为等待给定 key 被 push 而阻塞，
 * 那么将这个 key 的放进 server.ready_keys 列表里面。
 *
 * 注意 db->ready_keys 是一个哈希表，
 * 这可以避免在事务或者脚本中，将同一个 key 一次又一次添加到列表的情况出现。
 *
 * The list will be finally processed by handleClientsBlockedOnLists() 
 *
 * 这个列表最终会被 handleClientsBlockedOnLists() 函数处理。
 */
void signalListAsReady(redisDb *db, robj *key) {
    readyList *rl;

    /* No clients blocking for this key? No need to queue it. */
    // 没有客户端被这个键阻塞，直接返回
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    // 这个键已经被添加到 ready_keys 中了，直接返回
    if (dictFind(db->ready_keys,key) != NULL) return;

    /* Ok, we need to queue this key into server.ready_keys. */
    // 创建一个 readyList 结构，保存键和数据库
    // 然后将 readyList 添加到 server.ready_keys 中
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. 
     *
     * 将 key 添加到 c->db->ready_keys 集合中，防止重复添加
     */
    incrRefCount(key);
    serverAssert(dictAdd(db->ready_keys,key,NULL) == DICT_OK);
}

/* This is a helper function for handleClientsBlockedOnLists(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 * 
 * 函数对被阻塞的客户端 receiver 、造成阻塞的 key 、 key 所在的数据库 db
 * 以及一个值 value 和一个位置值 where 执行以下动作：
 *
 * 1) Provide the client with the 'value' element.
 *
 *    将 value 提供给 receiver
 *
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 *
 *    如果 dstkey 不为空（BRPOPLPUSH的情况），
 *    那么也将 value 推入到 dstkey 指定的列表中。
 *
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 *
 *    将 BRPOP 、 BLPOP 和可能有的 LPUSH 传播到 AOF 和同步节点
 *
 * The argument 'where' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element was popped fron the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * where 可能是 REDIS_TAIL 或者 REDIS_HEAD ，用于识别该 value 是从那个地方 POP
 * 出来，依靠这个参数，可以同样传播 BLPOP 或者 BRPOP 。
 * The function returns C_OK if we are able to serve the client, otherwise
 * C_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. 
 *
 * 如果一切成功，返回 REDIS_OK 。
 * 如果执行失败，那么返回 REDIS_ERR ，让 Redis 撤销对目标节点的 POP 操作。
 * 失败的情况只会出现在 BRPOPLPUSH 命令中，
 * 比如 POP 源列表成功，却发现 PUSH 的目标对象不是列表时，操作就会出现失败。
 */
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where)
{
    robj *argv[3];

    // 执行的是 BLPOP 或 BRPOP
    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        argv[0] = (where == LIST_HEAD) ? shared.lpop :
                                          shared.rpop;
        argv[1] = key;
        propagate((where == LIST_HEAD) ?
            server.lpopCommand : server.rpopCommand,
            db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        addReplyMultiBulkLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);
    // 执行的是 BRPOPLPUSH 
    } else {
        /* BRPOPLPUSH */
        // 取出目标对象
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey);
        if (!(dstobj &&
             checkType(receiver,dstobj,OBJ_LIST)))
        {
            /* Propagate the RPOP operation. */
            // 传播 RPOP 命令
            argv[0] = shared.rpop;
            argv[1] = key;
            propagate(server.rpopCommand,
                db->id,argv,2,
                PROPAGATE_AOF|
                PROPAGATE_REPL);
            // 将值推入到 dstobj 中，如果 dstobj 不存在，
            // 那么新创建一个
            rpoplpushHandlePush(receiver,dstkey,dstobj,
                value);
            /* Propagate the LPUSH operation. */
            // 传播 LPUSH 命令
            argv[0] = shared.lpush;
            argv[1] = dstkey;
            argv[2] = value;
            propagate(server.lpushCommand,
                db->id,argv,3,
                PROPAGATE_AOF|
                PROPAGATE_REPL);
        } else {
            /* BRPOPLPUSH failed because of wrong
             * destination type. */
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * 这个函数会在 Redis 每次执行完单个命令、事务块或 Lua 脚本之后调用。
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. 
 *
 * 对所有被阻塞在某个客户端的 key 来说，只要这个 key 被执行了某种 PUSH 操作
 * 那么这个 key 就会被放到 serve.ready_keys 去。
 * 
 * 这个函数会遍历整个 serve.ready_keys 链表，
 * 并将里面的 key 的元素弹出给被阻塞客户端，
 * 从而解除客户端的阻塞状态。
 *
 * 函数会一次又一次地进行迭代，
 * 因此它在执行 BRPOPLPUSH 命令的情况下也可以正常获取到正确的新被阻塞客户端。
 */
void handleClientsBlockedOnLists(void) {
    // 遍历整个 ready_keys 链表
    while(listLength(server.ready_keys) != 0) {
        list *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalListAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into BRPOPLPUSH. */
        // 备份旧的 ready_keys ，再给服务器端赋值一个新的
        l = server.ready_keys;
        server.ready_keys = listCreate();

        while(listLength(l) != 0) {
            // 取出 ready_keys 中的首个链表节点
            listNode *ln = listFirst(l);
            // 指向 readyList 结构
            readyList *rl = ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalListAsReady() against this key. */
            // 从 ready_keys 中移除就绪的 key
            dictDelete(rl->db->ready_keys,rl->key);

            /* If the key exists and it's a list, serve blocked clients
             * with data. */
            // 获取键对象，这个对象应该是非空的，并且是列表
            robj *o = lookupKeyWrite(rl->db,rl->key);
            if (o != NULL && o->type == OBJ_LIST) {
                dictEntry *de;

                /* We serve clients in the same order they blocked for
                 * this key, from the first blocked to the last. */
                // 取出所有被这个 key 阻塞的客户端
                de = dictFind(rl->db->blocking_keys,rl->key);
                if (de) {
                    list *clients = dictGetVal(de);
                    int numclients = listLength(clients);

                    while(numclients--) {
                        // 取出客户端
                        listNode *clientnode = listFirst(clients);
                        client *receiver = clientnode->value;
                        // 设置弹出的目标对象（只在 BRPOPLPUSH 时使用）
                        robj *dstkey = receiver->bpop.target;
                        // 从列表中弹出元素
                        // 弹出的位置取决于是执行 BLPOP 还是 BRPOP 或者 BRPOPLPUSH
                        int where = (receiver->lastcmd &&
                                     receiver->lastcmd->proc == blpopCommand) ?
                                    LIST_HEAD : LIST_TAIL;
                        robj *value = listTypePop(o,where);

                        // 还有元素可弹出（非 NULL）
                        if (value) {
                            /* Protect receiver->bpop.target, that will be
                             * freed by the next unblockClient()
                             * call. */
                            if (dstkey) incrRefCount(dstkey);
                            // 取消客户端的阻塞状态
                            unblockClient(receiver);

                            // 将值 value 推入到造成客户度 receiver 阻塞的 key 上
                            if (serveClientBlockedOnList(receiver,
                                rl->key,dstkey,rl->db,value,
                                where) == C_ERR)
                            {
                                /* If we failed serving the client we need
                                 * to also undo the POP operation. */
                                    listTypePush(o,value,where);
                            }

                            if (dstkey) decrRefCount(dstkey);
                            decrRefCount(value);
                        } else {
                            // 如果执行到这里，表示还有至少一个客户端被键所阻塞
                            // 这些客户端要等待对键的下次 PUSH
                            break;
                        }
                    }
                }
                // 如果列表元素已经为空，那么从数据库中将它删除
                if (listTypeLength(o) == 0) {
                    dbDelete(rl->db,rl->key);
                }
                /* We don't call signalModifiedKey() as it was already called
                 * when an element was pushed on the list. */
            }

            /* Free this item. */
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* We have the new list on place at this point. */
    }
}

/* Blocking RPOP/LPOP */
void blockingPopGenericCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    // 取出 timeout 参数
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != C_OK) return;

    // 遍历所有列表键
    for (j = 1; j < c->argc-1; j++) {
        // 取出列表键
        o = lookupKeyWrite(c->db,c->argv[j]);
        // 有非空列表？
        if (o != NULL) {
            if (o->type != OBJ_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                // 非空列表
                if (listTypeLength(o) != 0) {
                    /* Non empty list, this is like a non normal [LR]POP. */
                    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";
                    // 弹出值
                    robj *value = listTypePop(o,where);
                    serverAssert(value != NULL);

                    // 回复客户端
                    addReplyMultiBulkLen(c,2);
                    // 回复弹出元素的列表
                    addReplyBulk(c,c->argv[j]);
                    // 回复弹出值
                    addReplyBulk(c,value);
                    decrRefCount(value);
                    notifyKeyspaceEvent(NOTIFY_LIST,event,
                                        c->argv[j],c->db->id);
                    // 删除空列表
                    if (listTypeLength(o) == 0) {
                        dbDelete(c->db,c->argv[j]);
                        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                            c->argv[j],c->db->id);
                    }
                    signalModifiedKey(c->db,c->argv[j]);
                    server.dirty++;

                    /* Replicate it as an [LR]POP instead of B[LR]POP. */
                    // 传播一个 [LR]POP 而不是 B[LR]POP
                    rewriteClientCommandVector(c,2,
                        (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the list is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    // 如果命令在一个事务中执行，那么为了不产生死等待
    // 服务器只能向客户端发送一个空回复
    if (c->flags & CLIENT_MULTI) {
        addReply(c,shared.nullmultibulk);
        return;
    }

    /* If the list is empty or the key does not exists we must block */
    // 所有输入列表键都不存在，只能阻塞了
    blockForKeys(c, c->argv + 1, c->argc - 2, timeout, NULL);
}

void blpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_HEAD);
}

void brpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_TAIL);
}

void brpoplpushCommand(client *c) {
    mstime_t timeout;

    // 取出 timeout 参数
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != C_OK) return;

    // 取出列表键
    robj *key = lookupKeyWrite(c->db, c->argv[1]);

    // 键为空，阻塞
    if (key == NULL) {
        if (c->flags & CLIENT_MULTI) {
            /* Blocking against an empty list in a multi state
             * returns immediately. */
            addReply(c, shared.nullbulk);
        } else {
            /* The list is empty and the client blocks. */
            blockForKeys(c, c->argv + 1, 1, timeout, c->argv[2]);
        }
    // 键非空，执行 RPOPLPUSH
    } else {
        if (key->type != OBJ_LIST) {
            addReply(c, shared.wrongtypeerr);
        } else {
            /* The list exists and has elements, so
             * the regular rpoplpushCommand is executed. */
            serverAssertWithInfo(c,key,listTypeLength(key) > 0);
            rpoplpushCommand(c);
        }
    }
}
