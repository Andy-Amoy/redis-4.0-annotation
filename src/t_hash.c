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
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. 
 *
 * 对 argv 数组中的多个对象进行检查，
 * 看是否需要将对象的编码从 REDIS_ENCODING_ZIPLIST 转换成 REDIS_ENCODING_HT
 *
 * Note that we only check string encoded objects
 * as their string length can be queried in constant time. 
 *
 * 注意程序只检查字符串值，因为它们的长度可以在常数时间内取得。
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // 如果对象不是 ziplist 编码，那么直接返回
    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    // 检查所有输入对象，看它们的字符串值是否超过了指定长度
    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            // 将对象的编码转换成 REDIS_ENCODING_HT
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. 
 *
 * 从 ziplist 编码的 hash 中取出和 field 相对应的值。
 *
 * 参数：
 *  field   域
 *  vstr    值是字符串时，将它保存到这个指针
 *  vlen    保存字符串的长度
 *  ll      值是整数时，将它保存到这个指针
 *
 * 查找失败时，函数返回 -1 。
 * 查找成功时，返回 0 。
 */
int hashTypeGetFromZiplist(robj *o, sds field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    // 确保编码正确
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    // 取出未编码的域
    // 遍历 ziplist ，查找域的位置
    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // 定位包含域的节点
        fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // 域已经找到，取出和它相对应的值的位置
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    // 从 ziplist 节点中取出值
    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
        return 0;
    }

    // 没找到
    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned.
 * 从 REDIS_ENCODING_HT 编码的 hash 中取出和 field 相对应的值。
 *
 * 成功找到值时返回 0 ，没找到返回 -1 。
 */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    // 确保编码正确
    serverAssert(o->encoding == OBJ_ENCODING_HT);

    // 在字典中查找域（键）
    de = dictFind(o->ptr, field);
    // 键不存在
    if (de == NULL) return NULL;
    // 取出域（键）的值
    return dictGetVal(de);
    // 成功找到
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        if (hashTypeGetFromZiplist(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. */
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) return NULL;
    if (vstr) return createStringObject((char*)vstr,vlen);
    else return createStringObjectFromLongLong(vll);
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds aux;

        if ((aux = hashTypeGetFromHashTable(o, field)) != NULL)
            len = sdslen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. 
 *
 * 检查给定域 feild 是否存在于 hash 对象 o 中。
 *
 * 存在返回 1 ，不存在返回 0 。
 */
int hashTypeExists(robj *o, sds field) {
    // 检查 ziplist
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    // 检查字典
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeGetFromHashTable(o, field) != NULL) return 1;
    // 未知编码
    } else {
        serverPanic("Unknown hash encoding");
    }
    // 不存在
    return 0;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;

    // 添加到 ziplist
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        // 遍历整个 ziplist ，尝试查找并更新 field （如果它已经存在的话）
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 定位到域 field
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // 定位到域的值
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                // 标识这次操作为更新操作
                update = 1;

                /* Delete value */
                // 删除旧的键值对
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                // 添加新的键值对
                zl = ziplistInsert(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }

        // 如果这不是更新操作，那么这就是一个添加操作
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            // 将新的 field-value 对推入到 ziplist 的末尾
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        // 更新对象指针
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
        // 检查在添加操作完成之后，是否需要将 ZIPLIST 编码转换成 HT 编码
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    // 添加到字典
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictFind(o->ptr,field);
        if (de) {
            sdsfree(dictGetVal(de));
            if (flags & HASH_SET_TAKE_VALUE) {
                dictGetVal(de) = value;
                value = NULL;
            } else {
                dictGetVal(de) = sdsdup(value);
            }
            update = 1;
        } else {
            sds f,v;
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
                f = sdsdup(field);
            }
            if (flags & HASH_SET_TAKE_VALUE) {
                v = value;
                value = NULL;
            } else {
                v = sdsdup(value);
            }
            dictAdd(o->ptr,f,v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Delete an element from a hash.
 *
 * 将给定 field 及其 value 从哈希表中删除
 *
 * Return 1 on deleted and 0 on not found. 
 *
 * 删除成功返回 1 ，因为域不存在而造成的删除失败返回 0 。
 */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    // 从 ziplist 中删除
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 定位到域
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                // 删除域和值
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }
    // 从字典中删除
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            // 删除成功时，看字典是否需要收缩
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. 
 *
 * 返回哈希表的 field-value 对数量
 */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist 中，每个 field-value 对都需要使用两个节点来保存
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

/*
 * 创建一个哈希类型的迭代器
 * hashTypeIterator 类型定义在 redis.h
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  hashTypeIterator
 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    // 指向对象
    hi->subject = subject;
    // 记录编码
    hi->encoding = subject->encoding;

    // 以 ziplist 的方式初始化迭代器
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    // 以字典的方式初始化迭代器
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    // 返回迭代器
    return hi;
}

/*
 * 释放迭代器
 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    // 释放字典迭代器
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    // 释放 ziplist 迭代器
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 *
 * 获取哈希中的下一个节点，并将它保存到迭代器。
 *
 * could be found and C_ERR when the iterator reaches the end.
 *
 * 如果获取成功，返回 REDIS_OK ，
 *
 * 如果已经没有元素可获取（为空，或者迭代完毕），那么返回 REDIS_ERR 。
 */
int hashTypeNext(hashTypeIterator *hi) {
    // 迭代 ziplist
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        // 第一次执行时，初始化指针
        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        // 获取下一个迭代节点
        } else {
            /* Advance cursor */
            serverAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        // 迭代完毕，或者 ziplist 为空
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        // 记录值的指针
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        // 更新迭代器指针
        hi->fptr = fptr;
        hi->vptr = vptr;
    // 迭代字典
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    // 未知编码
    } else {
        serverPanic("Unknown hash encoding");
    }
    // 迭代成功
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. 
 *
 * 从 ziplist 编码的哈希中，取出迭代器指针当前指向节点的域或值。
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    // 确保编码正确
    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    // 取出键
    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    // 取出值
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`.
 * 根据迭代器的指针，从字典编码的哈希中取出所指向节点的 field 或者 value 。
 */
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    // 取出键
    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    // 取出值
    } else {
        return dictGetVal(hi->de);
    }
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds ele = hashTypeCurrentFromHashTable(hi, what);
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/*
 * 按 key 在数据库中查找并返回相应的哈希对象，
 * 如果对象不存在，那么创建一个新哈希对象并返回。
 */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    // 对象不存在，创建新的
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    // 对象存在，检查类型
    } else {
        if (o->type != OBJ_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    // 返回对象
    return o;
}

/*
 * 将一个 ziplist 编码的哈希对象 o 转换成其他编码
 */
void hashTypeConvertZiplist(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    // 如果输入是 ZIPLIST ，那么不做动作
    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    // 转换成 HT 编码
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // 创建哈希迭代器
        hi = hashTypeInitIterator(o);
        // 创建空白的新字典
        dict = dictCreate(&hashDictType, NULL);

        // 遍历整个 ziplist
        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;
            // 取出 ziplist 里的键
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            // 取出 ziplist 里的值
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            // 将键值对添加到字典
            ret = dictAdd(dict, key, value);
            if (ret != DICT_OK) {
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                serverPanic("Ziplist corruption detected");
            }
        }
        // 释放 ziplist 的迭代器
        hashTypeReleaseIterator(hi);
        // 释放对象原来的 ziplist
        zfree(o->ptr);
        // 更新哈希的编码和值对象
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*
 * 对哈希对象 o 的编码方式进行转换
 *
 * 目前只支持将 ZIPLIST 编码转换成 HT 编码
 */
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

void hsetCommand(client *c) {
    int update;
    robj *o;

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,3);
    // 设置 field 和 value 到 hash
    update = hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
    // 返回状态：显示 field-value 对是新添加还是更新
    addReply(c, update ? shared.czero : shared.cone);
    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    // 将服务器设为脏
    server.dirty++;
}

void hsetnxCommand(client *c) {
    robj *o;
    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,3);

    // 如果 field-value 对已经存在
    // 那么回复 0 
    if (hashTypeExists(o, c->argv[2]->ptr)) {
        addReply(c, shared.czero);
    // 否则，设置 field-value 对
    } else {
        // 设置
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
        // 回复 1 ，表示设置成功
        addReply(c, shared.cone);
        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);
        // 发送事件通知
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        // 将数据库设为脏
        server.dirty++;
    }
}

void hmsetCommand(client *c) {
    int i;
    robj *o;

    // field-value 参数必须成对出现
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 如果需要的话，转换哈希对象的编码
    hashTypeTryConversion(o,c->argv,2,c->argc-1);
    // 遍历并设置所有 field-value 对
    for (i = 2; i < c->argc; i += 2) {
        // 设置
        hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);
    }
    // 向客户端发送回复
    addReply(c, shared.ok);
    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    // 将数据库设为脏
    server.dirty++;
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    // 取出 incr 参数的值，并创建对象
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 取出 field 的当前值
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        // 如果值当前不存在，那么默认为 0
        value = 0;
    }

    // 检查计算是否会造成溢出
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    // 计算结果
    value += incr;
    // 为结果创建新的值对象
    new = sdsfromlonglong(value);
    // 关联键和新的值对象，如果已经有对象存在，那么用新对象替换它
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // 将计算结果用作回复
    addReplyLongLong(c,value);
    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    // 将数据库设为脏
    server.dirty++;
}

void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    // 取出 incr 参数
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // 取出或新创建哈希对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 取出值对象
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else {
        // 值对象不存在，默认值为 0
        value = 0;
    }

    // 计算结果
    value += incr;

    char buf[256];
    int len = ld2string(buf,sizeof(buf),value,1);
    new = sdsnewlen(buf,len);
    // 关联键和新的值对象，如果已经有对象存在，那么用新对象替换它
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // 返回新的值对象作为回复
    addReplyBulkCBuffer(c,buf,len);
    // 发送键修改信号
    signalModifiedKey(c->db,c->argv[1]);
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    // 将数据库设置脏
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 在传播 INCRBYFLOAT 命令时，总是用 SET 命令来替换 INCRBYFLOAT 命令
    // 从而防止因为不同的浮点精度和格式化造成 AOF 重启时的数据不一致
    robj *aux, *newobj;
    aux = createStringObject("HSET",4);
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

/*
 * 辅助函数：将哈希中域 field 的值添加到回复中
 */
static void addHashFieldToReply(client *c, robj *o, sds field) {
    int ret;

    // 对象不存在
    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    // ziplist 编码
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 取出值
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    // 字典
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // 取出值
        sds value = hashTypeGetFromHashTable(o, field);
        if (value == NULL)
            addReply(c, shared.nullbulk);
        else
            addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hgetCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 取出并返回域的值
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    // 取出哈希对象
    o = lookupKeyRead(c->db, c->argv[1]);
    // 对象存在，检查类型
    if (o != NULL && o->type != OBJ_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    // 获取多个 field 的值
    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    // 取出对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 删除指定域值对
    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            // 成功删除一个域值对时进行计数
            deleted++;
            // 如果哈希已经为空，那么删除这个对象
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    // 只要有至少一个域值对被修改了，那么执行以下代码
    if (deleted) {
        // 发送键修改信号
        signalModifiedKey(c->db,c->argv[1]);
        // 发送事件通知
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        // 发送事件通知
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        // 将数据库设为脏
        server.dirty += deleted;
    }
    // 将成功删除的域值对数量作为结果返回给客户端
    addReplyLongLong(c,deleted);
}

void hlenCommand(client *c) {
    robj *o;

    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 回复
    addReplyLongLong(c,hashTypeLength(o));
}

void hstrlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

/*
 * 从迭代器当前指向的节点中取出哈希的 field 或 value
 */
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    // 处理 ZIPLIST
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    // 处理 HT
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;

    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,OBJ_HASH)) return;

    // 计算要取出的元素数量
    if (flags & OBJ_HASH_KEY) multiplier++;
    if (flags & OBJ_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    // 迭代节点，并取出元素
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        // 取出键
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        // 取出值
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    // 释放迭代器
    hashTypeReleaseIterator(hi);
    serverAssert(count == length);
}

void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

void hexistsCommand(client *c) {
    robj *o;
    // 取出哈希对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 检查给定域是否存在
    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    scanGenericCommand(c,o,cursor);
}
