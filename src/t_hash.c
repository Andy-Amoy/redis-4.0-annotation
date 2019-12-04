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
 * �� argv �����еĶ��������м�飬
 * ���Ƿ���Ҫ������ı���� REDIS_ENCODING_ZIPLIST ת���� REDIS_ENCODING_HT
 *
 * Note that we only check string encoded objects
 * as their string length can be queried in constant time. 
 *
 * ע�����ֻ����ַ���ֵ����Ϊ���ǵĳ��ȿ����ڳ���ʱ����ȡ�á�
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // ��������� ziplist ���룬��ôֱ�ӷ���
    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    // �������������󣬿����ǵ��ַ���ֵ�Ƿ񳬹���ָ������
    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            // ������ı���ת���� REDIS_ENCODING_HT
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. 
 *
 * �� ziplist ����� hash ��ȡ���� field ���Ӧ��ֵ��
 *
 * ������
 *  field   ��
 *  vstr    ֵ���ַ���ʱ���������浽���ָ��
 *  vlen    �����ַ����ĳ���
 *  ll      ֵ������ʱ���������浽���ָ��
 *
 * ����ʧ��ʱ���������� -1 ��
 * ���ҳɹ�ʱ������ 0 ��
 */
int hashTypeGetFromZiplist(robj *o, sds field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    // ȷ��������ȷ
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    // ȡ��δ�������
    // ���� ziplist ���������λ��
    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // ��λ������Ľڵ�
        fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // ���Ѿ��ҵ���ȡ���������Ӧ��ֵ��λ��
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    // �� ziplist �ڵ���ȡ��ֵ
    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
        return 0;
    }

    // û�ҵ�
    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned.
 * �� REDIS_ENCODING_HT ����� hash ��ȡ���� field ���Ӧ��ֵ��
 *
 * �ɹ��ҵ�ֵʱ���� 0 ��û�ҵ����� -1 ��
 */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    // ȷ��������ȷ
    serverAssert(o->encoding == OBJ_ENCODING_HT);

    // ���ֵ��в����򣨼���
    de = dictFind(o->ptr, field);
    // ��������
    if (de == NULL) return NULL;
    // ȡ���򣨼�����ֵ
    return dictGetVal(de);
    // �ɹ��ҵ�
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
 * �������� feild �Ƿ������ hash ���� o �С�
 *
 * ���ڷ��� 1 �������ڷ��� 0 ��
 */
int hashTypeExists(robj *o, sds field) {
    // ��� ziplist
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    // ����ֵ�
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeGetFromHashTable(o, field) != NULL) return 1;
    // δ֪����
    } else {
        serverPanic("Unknown hash encoding");
    }
    // ������
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

    // ��ӵ� ziplist
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        // �������� ziplist �����Բ��Ҳ����� field ��������Ѿ����ڵĻ���
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // ��λ���� field
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // ��λ�����ֵ
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                // ��ʶ��β���Ϊ���²���
                update = 1;

                /* Delete value */
                // ɾ���ɵļ�ֵ��
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                // ����µļ�ֵ��
                zl = ziplistInsert(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }

        // ����ⲻ�Ǹ��²�������ô�����һ����Ӳ���
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            // ���µ� field-value �����뵽 ziplist ��ĩβ
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        // ���¶���ָ��
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
        // �������Ӳ������֮���Ƿ���Ҫ�� ZIPLIST ����ת���� HT ����
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    // ��ӵ��ֵ�
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
 * ������ field ���� value �ӹ�ϣ����ɾ��
 *
 * Return 1 on deleted and 0 on not found. 
 *
 * ɾ���ɹ����� 1 ����Ϊ�򲻴��ڶ���ɵ�ɾ��ʧ�ܷ��� 0 ��
 */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    // �� ziplist ��ɾ��
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // ��λ����
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                // ɾ�����ֵ
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }
    // ���ֵ���ɾ��
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            // ɾ���ɹ�ʱ�����ֵ��Ƿ���Ҫ����
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. 
 *
 * ���ع�ϣ��� field-value ������
 */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist �У�ÿ�� field-value �Զ���Ҫʹ�������ڵ�������
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

/*
 * ����һ����ϣ���͵ĵ�����
 * hashTypeIterator ���Ͷ����� redis.h
 *
 * ���Ӷȣ�O(1)
 *
 * ����ֵ��
 *  hashTypeIterator
 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    // ָ�����
    hi->subject = subject;
    // ��¼����
    hi->encoding = subject->encoding;

    // �� ziplist �ķ�ʽ��ʼ��������
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    // ���ֵ�ķ�ʽ��ʼ��������
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    // ���ص�����
    return hi;
}

/*
 * �ͷŵ�����
 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    // �ͷ��ֵ������
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    // �ͷ� ziplist ������
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 *
 * ��ȡ��ϣ�е���һ���ڵ㣬���������浽��������
 *
 * could be found and C_ERR when the iterator reaches the end.
 *
 * �����ȡ�ɹ������� REDIS_OK ��
 *
 * ����Ѿ�û��Ԫ�ؿɻ�ȡ��Ϊ�գ����ߵ�����ϣ�����ô���� REDIS_ERR ��
 */
int hashTypeNext(hashTypeIterator *hi) {
    // ���� ziplist
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        // ��һ��ִ��ʱ����ʼ��ָ��
        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        // ��ȡ��һ�������ڵ�
        } else {
            /* Advance cursor */
            serverAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        // ������ϣ����� ziplist Ϊ��
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        // ��¼ֵ��ָ��
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        // ���µ�����ָ��
        hi->fptr = fptr;
        hi->vptr = vptr;
    // �����ֵ�
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    // δ֪����
    } else {
        serverPanic("Unknown hash encoding");
    }
    // �����ɹ�
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. 
 *
 * �� ziplist ����Ĺ�ϣ�У�ȡ��������ָ�뵱ǰָ��ڵ�����ֵ��
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    // ȷ��������ȷ
    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    // ȡ����
    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    // ȡ��ֵ
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`.
 * ���ݵ�������ָ�룬���ֵ����Ĺ�ϣ��ȡ����ָ��ڵ�� field ���� value ��
 */
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    // ȡ����
    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    // ȡ��ֵ
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
 * �� key �����ݿ��в��Ҳ�������Ӧ�Ĺ�ϣ����
 * ������󲻴��ڣ���ô����һ���¹�ϣ���󲢷��ء�
 */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    // ���󲻴��ڣ������µ�
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    // ������ڣ��������
    } else {
        if (o->type != OBJ_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    // ���ض���
    return o;
}

/*
 * ��һ�� ziplist ����Ĺ�ϣ���� o ת������������
 */
void hashTypeConvertZiplist(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    // ��������� ZIPLIST ����ô��������
    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    // ת���� HT ����
    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // ������ϣ������
        hi = hashTypeInitIterator(o);
        // �����հ׵����ֵ�
        dict = dictCreate(&hashDictType, NULL);

        // �������� ziplist
        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;
            // ȡ�� ziplist ��ļ�
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            // ȡ�� ziplist ���ֵ
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            // ����ֵ����ӵ��ֵ�
            ret = dictAdd(dict, key, value);
            if (ret != DICT_OK) {
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                serverPanic("Ziplist corruption detected");
            }
        }
        // �ͷ� ziplist �ĵ�����
        hashTypeReleaseIterator(hi);
        // �ͷŶ���ԭ���� ziplist
        zfree(o->ptr);
        // ���¹�ϣ�ı����ֵ����
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*
 * �Թ�ϣ���� o �ı��뷽ʽ����ת��
 *
 * Ŀǰֻ֧�ֽ� ZIPLIST ����ת���� HT ����
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

    // ȡ�����´�����ϣ����
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // �����Ҫ�Ļ���ת����ϣ����ı���
    hashTypeTryConversion(o,c->argv,2,3);
    // ���� field �� value �� hash
    update = hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
    // ����״̬����ʾ field-value ��������ӻ��Ǹ���
    addReply(c, update ? shared.czero : shared.cone);
    // ���ͼ��޸��ź�
    signalModifiedKey(c->db,c->argv[1]);
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    // ����������Ϊ��
    server.dirty++;
}

void hsetnxCommand(client *c) {
    robj *o;
    // ȡ�����´�����ϣ����
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // �����Ҫ�Ļ���ת����ϣ����ı���
    hashTypeTryConversion(o,c->argv,2,3);

    // ��� field-value ���Ѿ�����
    // ��ô�ظ� 0 
    if (hashTypeExists(o, c->argv[2]->ptr)) {
        addReply(c, shared.czero);
    // �������� field-value ��
    } else {
        // ����
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
        // �ظ� 1 ����ʾ���óɹ�
        addReply(c, shared.cone);
        // ���ͼ��޸��ź�
        signalModifiedKey(c->db,c->argv[1]);
        // �����¼�֪ͨ
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        // �����ݿ���Ϊ��
        server.dirty++;
    }
}

void hmsetCommand(client *c) {
    int i;
    robj *o;

    // field-value ��������ɶԳ���
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    // ȡ�����´�����ϣ����
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // �����Ҫ�Ļ���ת����ϣ����ı���
    hashTypeTryConversion(o,c->argv,2,c->argc-1);
    // �������������� field-value ��
    for (i = 2; i < c->argc; i += 2) {
        // ����
        hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);
    }
    // ��ͻ��˷��ͻظ�
    addReply(c, shared.ok);
    // ���ͼ��޸��ź�
    signalModifiedKey(c->db,c->argv[1]);
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    // �����ݿ���Ϊ��
    server.dirty++;
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    // ȡ�� incr ������ֵ������������
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // ȡ�����´�����ϣ����
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // ȡ�� field �ĵ�ǰֵ
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        // ���ֵ��ǰ�����ڣ���ôĬ��Ϊ 0
        value = 0;
    }

    // �������Ƿ��������
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    // ������
    value += incr;
    // Ϊ��������µ�ֵ����
    new = sdsfromlonglong(value);
    // ���������µ�ֵ��������Ѿ��ж�����ڣ���ô���¶����滻��
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // �������������ظ�
    addReplyLongLong(c,value);
    // ���ͼ��޸��ź�
    signalModifiedKey(c->db,c->argv[1]);
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    // �����ݿ���Ϊ��
    server.dirty++;
}

void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
    // ȡ�� incr ����
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // ȡ�����´�����ϣ����
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // ȡ��ֵ����
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
        // ֵ���󲻴��ڣ�Ĭ��ֵΪ 0
        value = 0;
    }

    // ������
    value += incr;

    char buf[256];
    int len = ld2string(buf,sizeof(buf),value,1);
    new = sdsnewlen(buf,len);
    // ���������µ�ֵ��������Ѿ��ж�����ڣ���ô���¶����滻��
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // �����µ�ֵ������Ϊ�ظ�
    addReplyBulkCBuffer(c,buf,len);
    // ���ͼ��޸��ź�
    signalModifiedKey(c->db,c->argv[1]);
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    // �����ݿ�������
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // �ڴ��� INCRBYFLOAT ����ʱ�������� SET �������滻 INCRBYFLOAT ����
    // �Ӷ���ֹ��Ϊ��ͬ�ĸ��㾫�Ⱥ͸�ʽ����� AOF ����ʱ�����ݲ�һ��
    robj *aux, *newobj;
    aux = createStringObject("HSET",4);
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

/*
 * ��������������ϣ���� field ��ֵ��ӵ��ظ���
 */
static void addHashFieldToReply(client *c, robj *o, sds field) {
    int ret;

    // ���󲻴���
    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    // ziplist ����
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // ȡ��ֵ
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

    // �ֵ�
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // ȡ��ֵ
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

    // ȡ�����������ֵ
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    // ȡ����ϣ����
    o = lookupKeyRead(c->db, c->argv[1]);
    // ������ڣ��������
    if (o != NULL && o->type != OBJ_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    // ��ȡ��� field ��ֵ
    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    // ȡ������
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // ɾ��ָ����ֵ��
    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            // �ɹ�ɾ��һ����ֵ��ʱ���м���
            deleted++;
            // �����ϣ�Ѿ�Ϊ�գ���ôɾ���������
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    // ֻҪ������һ����ֵ�Ա��޸��ˣ���ôִ�����´���
    if (deleted) {
        // ���ͼ��޸��ź�
        signalModifiedKey(c->db,c->argv[1]);
        // �����¼�֪ͨ
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        // �����¼�֪ͨ
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        // �����ݿ���Ϊ��
        server.dirty += deleted;
    }
    // ���ɹ�ɾ������ֵ��������Ϊ������ظ��ͻ���
    addReplyLongLong(c,deleted);
}

void hlenCommand(client *c) {
    robj *o;

    // ȡ����ϣ����
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // �ظ�
    addReplyLongLong(c,hashTypeLength(o));
}

void hstrlenCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

/*
 * �ӵ�������ǰָ��Ľڵ���ȡ����ϣ�� field �� value
 */
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    // ���� ZIPLIST
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    // ���� HT
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

    // ȡ����ϣ����
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,OBJ_HASH)) return;

    // ����Ҫȡ����Ԫ������
    if (flags & OBJ_HASH_KEY) multiplier++;
    if (flags & OBJ_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    // �����ڵ㣬��ȡ��Ԫ��
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        // ȡ����
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        // ȡ��ֵ
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    // �ͷŵ�����
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
    // ȡ����ϣ����
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // ���������Ƿ����
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
