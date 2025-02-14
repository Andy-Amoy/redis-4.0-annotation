/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#ifndef DICT_BENCHMARK_MAIN
#include "redisassert.h"
#else
#include <assert.h>
#endif

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * 通过 dictEnableResize() 和 dictDisableResize() 两个函数，
 * 程序可以手动地允许或阻止哈希表进行 rehash ，
 * 这在 Redis 使用子进程进行保存操作时，可以有效地利用 copy-on-write 机制。
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio.
 * 需要注意的是，并非所有 rehash 都会被 dictDisableResize 阻止：
 * 如果已使用节点的数量和字典大小之间的比率，
 * 大于字典强制 rehash 比率 dict_force_resize_ratio ，
 * 那么 rehash 仍然会（强制）进行。
 */
// 指示字典是否启用 rehash 的标识

 //通过dictEnableResize与dictDisableResize可以启用或者禁用hash表rehash过程
 //若dict_can_resize=0，当条件满足时仍可以进行进行rehash重分配操作
static int dict_can_resize = 1;
// 强制 rehash 的比率
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */
//私有方法
//字典扩展
static int _dictExpandIfNeeded(dict *ht);
//计算字典扩展或者收缩的新size
static unsigned long _dictNextPower(unsigned long size);
//计算key对应的hash表的索引
static int _dictKeyIndex(dict *ht, const void *key, unsigned int hash, dictEntry **existing);
//字典初始化
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */
//hash函数seed数组
static uint8_t dict_hash_function_seed[16];

//设置hash函数seed数组
void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed,seed,sizeof(dict_hash_function_seed));
}
//获取hash函数seed数组
uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */
//默认hash函数,具体实现见siphash.c
uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

//字典key hash
uint64_t dictGenHashFunction(const void *key, int len) {
    return siphash(key,len,dict_hash_function_seed);
}
//字典key hash
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf,len,dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
/*
 * 重置（或初始化）给定哈希表的各项属性值
 *
 * p.s. 上面的英文注释已经过期
 *
 * T = O(1)
 */
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* Create a new hash table */
/*
 * 创建一个新的字典
 *
 * T = O(1)
 */
dict *dictCreate(dictType *type,
        void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));//为字典分配内存空间

    _dictInit(d,type,privDataPtr);//初始化字典
    return d;
}

/* Initialize the hash table */
/*
 * 初始化哈希表
 *
 * T = O(1)
 */
int _dictInit(dict *d, dictType *type,
        void *privDataPtr)
{
    // 初始化两个哈希表的各项属性值
    // 但暂时还不分配内存给哈希表数组
    _dictReset(&d->ht[0]);//初始化第一张hash表，非rehash情况下使用的hash表
    _dictReset(&d->ht[1]);//初始化第二张hash表，rehash过程中与ht[0]共同使用的hash表
    // 设置类型特定函数
    d->type = type; //设置hash表类型
    d->privdata = privDataPtr; //设置私有数据
    // 设置哈希表 rehash 状态
    d->rehashidx = -1; //初始值
    // 设置字典的安全迭代器数量
    d->iterators = 0; //初始值
    return DICT_OK; //返回DICT_OK
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
/*
 * 缩小给定字典
 * 让它的已用节点数和字典大小之间的比率接近 1:1
 *
 * 返回 DICT_ERR 表示字典已经在 rehash ，或者 dict_can_resize 为假。
 *
 * 成功创建体积更小的 ht[1] ，可以开始 resize 时，返回 DICT_OK。
 *
 * T = O(N)
 */
int dictResize(dict *d)
{
    int minimal;
    //如果dict_can_resize=0，或者字典正在进行rehash操作，则返回DICT_ERR
    // 不能在关闭 rehash 或者正在 rehash 的时候调用
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;
    // 计算让比率接近 1：1 所需要的最少节点数量
    minimal = d->ht[0].used; //获取已使用的hash表节点数
    if (minimal < DICT_HT_INITIAL_SIZE) //若值小于DICT_HT_INITIAL_SIZE,则直接使用DICT_HT_INITIAL_SIZE大小进行扩展
        minimal = DICT_HT_INITIAL_SIZE;
    // 调整字典的大小
    // T = O(N)
    return dictExpand(d, minimal); //执行字典扩展操作
}

/* Expand or create the hash table */
/*
 * 创建一个新的哈希表，并根据字典的情况，选择以下其中一个动作来进行：
 *
 * 1) 如果字典的 0 号哈希表为空，那么将新哈希表设置为 0 号哈希表
 * 2) 如果字典的 0 号哈希表非空，那么将新哈希表设置为 1 号哈希表，
 *    并打开字典的 rehash 标识，使得程序可以开始对字典进行 rehash
 *
 * size 参数不够大，或者 rehash 已经在进行时，返回 DICT_ERR 。
 *
 * 成功创建 0 号哈希表，或者 1 号哈希表时，返回 DICT_OK 。
 *
 * T = O(N)
 */
int dictExpand(dict *d, unsigned long size)
{
    // 新哈希表
    dictht n; /* the new hash table */
    // 根据 size 参数，计算哈希表的大小
    // T = O(1)
    unsigned long realsize = _dictNextPower(size);

    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    //若字典正在执行rehash操作或者扩展的size小于hash表已使用的节点数，则返回DICT_ERR
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    /* Rehashing to the same table size is not useful. */
    //如果准备扩展的新hash表的大小与原hash大小一致，则返回DICT_ERR
    if (realsize == d->ht[0].size) return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    //初始化新的hash表
    n.size = realsize; //设置hash表大小
    n.sizemask = realsize-1;//设置hash表掩码
    // T = O(N)
    n.table = zcalloc(realsize*sizeof(dictEntry*)); //申请内存空间
    n.used = 0; //初始值

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    //如果ht[0]为NULL，则直接赋值新表指针
    // 如果 0 号哈希表为空，那么这是一次初始化：
    // 程序将新哈希表赋给 0 号哈希表的指针，然后字典就可以开始处理键值对了。
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    //若原表不为NULL，则原hash表ht[1]置为新表,设置rehashidx=0,表示开始rehash操作
    // 如果 0 号哈希表非空，那么这是一次 rehash ：
    // 程序将新哈希表设置为 1 号哈希表，
    // 并将字典的 rehash 标识打开，让程序可以开始对字典进行 rehash
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;//返回DICT_OK
    /* 顺带一提，上面的代码可以重构成以下形式：
    
    if (d->ht[0].table == NULL) {
        // 初始化
        d->ht[0] = n;
    } else {
        // rehash 
        d->ht[1] = n;
        d->rehashidx = 0;
    }

    return DICT_OK;

    */
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * 执行 N 步渐进式 rehash 。
 *
 * 返回 1 表示仍有键需要从 0 号哈希表移动到 1 号哈希表，
 * 返回 0 则表示所有键都已经迁移完毕。
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time.
 //字典rehash, 渐进式rehash，如果返回0，表示rehash已完成，若返回1，则表示ht[0]还有剩余hash节点需要rehash至ht[1]
 * 注意，每步 rehash 都是以一个哈希表索引（桶）作为单位的，
 * 一个桶里可能会有多个节点，
 * 被 rehash 的桶里的所有节点都会被移动到新哈希表。
 *
 * T = O(N)
 */
int dictRehash(dict *d, int n) {
    //空hash节点最大值
    int empty_visits = n*10; /* Max number of empty buckets to visit. */
    //若当前不在rehash过程，则表示已rehash完毕，返回0
    if (!dictIsRehashing(d)) return 0;
    //当前ht[0]仍有剩余hash节点,执行n步批量rehash
    // 进行 N 步迁移
    // T = O(N)
    while(n-- && d->ht[0].used != 0) {
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        //断言rehashidx未超过ht[0]表大小
        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        //轮空hash表当中为NULL的hash节点
        while(d->ht[0].table[d->rehashidx] == NULL) {
            d->rehashidx++;
            if (--empty_visits == 0) return 1; //空hash节点超过empty_visits则返回1，等待下一轮渐进式rehash
        }
        //当前不为NULL的需要rehash的hash节点
        de = d->ht[0].table[d->rehashidx];
        /* Move all the keys in this bucket from the old to the new hash HT */
        //rehash hash节点slot的键冲突节点列表
        while(de) {
            unsigned int h;

            nextde = de->next; //设置下一节点
            /* Get the index in the new hash table */
            // 计算新哈希表的哈希值，以及节点插入的索引位置
            h = dictHashKey(d, de->key) & d->ht[1].sizemask; //计算ht[1] rehash之后的索引值
            //将rehash之后的新节点，插入index位置的链表首部位置
            de->next = d->ht[1].table[h]; 
            d->ht[1].table[h] = de;
            //减少ht[0] used值，增加ht[1] used值
            // 更新计数器
            d->ht[0].used--;
            d->ht[1].used++;
            de = nextde; //设置当前节点为下一节点
        }
        // 将刚迁移完的哈希表索引的指针设为空
        d->ht[0].table[d->rehashidx] = NULL; //节点rehash操作，置为NULL
        d->rehashidx++; //rehash索引值+1
    }

    /* Check if we already rehashed the whole table... */
    //检查rehash是否完成
    if (d->ht[0].used == 0) {
        zfree(d->ht[0].table); //释放ht[0]表
        d->ht[0] = d->ht[1]; //将ht[1]新表赋值给ht[0]表
        _dictReset(&d->ht[1]);//重置ht[1]指针
        d->rehashidx = -1; //设置rehashidx=-1
        return 0; //返回0，表示rehash操作完成
    }

    /* More to rehash... */
    return 1; //返回1，表示还需进行下一轮渐进式rehash
}

/*
 * 返回以毫秒为单位的 UNIX 时间戳
 *
 * T = O(1)
 */
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash for an amount of time between ms milliseconds and ms+1 milliseconds */
/*
 * 在给定毫秒数内，以 100 步为单位，对字典进行 rehash 。
 *
 * T = O(N)
 */
int dictRehashMilliseconds(dict *d, int ms) {
    // 记录开始时间
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while(dictRehash(d,100)) { //执行小批量rehash操作
        rehashes += 100;
        // 如果时间已过，跳出
        if (timeInMilliseconds()-start > ms) break; //超时则返回
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if there are
 * no safe iterators bound to our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * 在字典不存在安全迭代器的情况下，对字典进行单步 rehash 。
 *
 * 字典有安全迭代器的情况下不能进行 rehash ，
 * 因为两种不同的迭代和修改操作可能会弄乱字典。
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used. 
 *
 * 这个函数被多个通用的查找、更新操作调用，
 * 它可以让字典在被使用的同时进行 rehash 。
 *
 * T = O(1)
 */
 //执行其他操作时，若符合rehash条件，则会触发rehash操作
static void _dictRehashStep(dict *d) {
    if (d->iterators == 0) dictRehash(d,1);
}

/* Add an element to the target hash table */
/*
 * 尝试将给定键值对添加到字典中
 *
 * 只有给定键 key 不存在于字典时，添加操作才会成功
 *
 * 添加成功返回 DICT_OK ，失败返回 DICT_ERR
 *
 * 最坏 T = O(N) ，平滩 O(1) 
 */
int dictAdd(dict *d, void *key, void *val)
{
    //调用dictAddRaw获取hash节点
    // T = O(N)
    dictEntry *entry = dictAddRaw(d,key,NULL);
    //若获取失败，则返回DICT_ERR
    if (!entry) return DICT_ERR;
    // 键不存在，设置节点的值
    // T = O(1)
    dictSetVal(d, entry, val);
    // 添加成功
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as he wishes.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
/*
 * 尝试将键插入到字典中
 *
 * 如果键已经在字典存在，那么返回 NULL
 *
 * 如果键不存在，那么程序创建新的哈希节点，
 * 将节点和键关联，并插入到字典，然后返回节点本身。
 *
 * T = O(N)
 */

 //新增键值对
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    int index;
    dictEntry *entry;
    dictht *ht;

    // 如果条件允许的话，进行单步 rehash
    // T = O(1)
    if (dictIsRehashing(d)) _dictRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    //获取新键值对的index值，如果index=-1,则表示该键已存在,此时返回NULL
    if ((index = _dictKeyIndex(d, key, dictHashKey(d,key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    //若正在进行rehash操作，则使用ht[1]表，ht[0]不支持插入新hash节点
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    //分配新节点内存空间
    entry = zmalloc(sizeof(*entry));
    //将新节点插入index对应冲突hash链表的首部
    entry->next = ht->table[index];
    ht->table[index] = entry;
    //ht used值+1
    ht->used++;

    /* Set the hash entry fields. */
    //设置新节点key
    dictSetKey(d, entry, key);
    return entry;
}

/* Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 * 将给定的键值对添加到字典中，如果键已经存在，那么删除旧有的键值对。
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. 
 *
 * 如果键值对为全新添加，那么返回 1 。
 * 如果键值对是通过对原有的键值对更新得来的，那么返回 0 。
 *
 * T = O(N)
 */
 //替换原键值对
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will suceed. */
    //获取字典对应key的hash节点，若存在，则更新对应值
    entry = dictAddRaw(d,key,&existing);
    if (entry) {
        dictSetVal(d, entry, val);
        return 1;
    }

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
     //设置新值，释放旧值
    // 先保存原有的值的指针
    auxentry = *existing;
    // 然后设置新的值
    // T = O(1)
    dictSetVal(d, existing, val);
    // 然后释放旧值
    // T = O(1)
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
/*
 * dictAddRaw() 根据给定 key 释放存在，执行以下动作：
 *
 * 1) key 已经存在，返回包含该 key 的字典节点
 * 2) key 不存在，那么将 key 添加到字典
 *
 * 不论发生以上的哪一种情况，
 * dictAddRaw() 都总是返回包含给定 key 的字典节点。
 *
 * T = O(N)
 */
 //返回新增或者搜索的节点
dictEntry *dictAddOrFind(dict *d, void *key) {
    dictEntry *entry, *existing;
    entry = dictAddRaw(d,key,&existing);
    return entry ? entry : existing;
}

/* Search and remove an element. This is an helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions. */
/*
 * 查找并删除包含给定键的节点
 *
 * 参数 nofree 决定是否调用键和值的释放函数
 * 0 表示调用，1 表示不调用
 *
 * 找到并成功删除返回 DICT_OK ，没找到则返回 DICT_ERR
 *
 * T = O(1)
 */
 //删除键值对
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    unsigned int h, idx;
    dictEntry *he, *prevHe;
    int table;

    // 字典（的哈希表）为空
    if (d->ht[0].used == 0 && d->ht[1].used == 0) return NULL; //若ht[0]与ht[1]均为空表，则返回NULL
    //若字典正在进行rehash操作，则执行一次rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);
    //计算hash key
    h = dictHashKey(d, key);

    //删除两张表当中对应的键值对
    // T = O(1)
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask; //计算索引
        // 指向该索引上的链表
        he = d->ht[table].table[idx];
        prevHe = NULL;
        // 遍历链表上的所有节点
        // T = O(1)
        while(he) {
            //若待删除hash存在
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                // 超找目标节点
                /* Unlink the element from the list */
                //将待删除hash节点从hash表中移除
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;
                if (!nofree) { //若nofree=0,则释放相关内存空间
                    dictFreeKey(d, he); //释放键空间
                    dictFreeVal(d, he);//释放值空间
                    zfree(he); //释放hash节点
                }
                d->ht[table].used--; //减少节点使用数
                // 返回已找到信号
                return he;
            }
            prevHe = he; //修改遍历指针
            he = he->next;
        }
        // 如果执行到这里，说明在 0 号哈希表中找不到给定键
        // 那么根据字典是否正在进行 rehash ，决定要不要查找 1 号哈希表
        if (!dictIsRehashing(d)) break;//若字典未执行rehash操作，则跳出循环
    }
    return NULL; /* not found */ //未找到对应key键的hash节点
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
/*
 * 从字典中删除包含给定键的节点
 * 
 * 并且调用键值的释放函数来删除键值
 *
 * 找到并成功删除返回 DICT_OK ，没找到则返回 DICT_ERR
 * T = O(1)
 */
//删除键值对,释放该节点内存空间
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
 //删除键值对,不释放该节点内存空间
dictEntry *dictUnlink(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL. */
//释放非Link hash节点
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictionary */
/*
 * 删除哈希表上的所有节点，并重置哈希表的各项属性
 *
 * T = O(N)
 */
//清空字典
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    /* Free all the elements */
    // 遍历整个哈希表
    // T = O(N)
    for (i = 0; i < ht->size && ht->used > 0; i++) {//遍历链表内所有节点
        dictEntry *he, *nextHe;

        if (callback && (i & 65535) == 0) callback(d->privdata);

        // 跳过空索引
        if ((he = ht->table[i]) == NULL) continue;
        // 遍历整个链表
        // T = O(1)
        while(he) { //释放冲突链表内存
            nextHe = he->next;
            // 删除键
            dictFreeKey(d, he);
            // 删除值
            dictFreeVal(d, he);
            // 释放节点
            zfree(he);
            // 更新已使用节点计数
            ht->used--;
            // 处理下个节点
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    // 释放哈希表结构
    zfree(ht->table); //清空表
    /* Re-initialize the table */
    _dictReset(ht);//重置表
    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table */
/*
 * 删除并释放整个字典
 *
 * T = O(N)
 */
void dictRelease(dict *d)
{
    // 删除并清空两个哈希表
    _dictClear(d,&d->ht[0],NULL);
    _dictClear(d,&d->ht[1],NULL);
    // 释放节点结构
    zfree(d);
}

/*
 * 返回字典中包含键 key 的节点
 *
 * 找到返回节点，找不到返回 NULL
 *
 * T = O(1)
 */
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    unsigned int h, idx, table;
    //若字典为空，则返回NULL
    if (d->ht[0].used + d->ht[1].used == 0) return NULL; /* dict is empty */
    //如果正在进行rehash，则执行一次rehash操作
    if (dictIsRehashing(d)) _dictRehashStep(d);
    //计算hash值
    h = dictHashKey(d, key);
    //遍历ht[0]与ht[1]两张表
    // 在字典的哈希表中查找这个键
    // T = O(1)
    for (table = 0; table <= 1; table++) {
        // 计算索引值
        idx = h & d->ht[table].sizemask;
        // 遍历给定索引上的链表的所有节点，查找 key
        he = d->ht[table].table[idx];
        // T = O(1)
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        // 如果程序遍历完 0 号哈希表，仍然没找到指定的键的节点
        // 那么程序会检查字典是否在进行 rehash ，
        // 然后才决定是直接返回 NULL ，还是继续查找 1 号哈希表
        if (!dictIsRehashing(d)) return NULL;
    }
    // 进行到这里时，说明两个哈希表都没找到
    return NULL;
}

/*
 * 获取包含给定键的节点的值
 *
 * 如果节点不为空，返回节点的值
 * 否则返回 NULL
 *
 * T = O(1)
 */
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    // T = O(1)
    he = dictFind(d,key); //根据key找到hash节点
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
 //64位数值,表示给定时间的字典状态,通过hash表少量属性值进行计算
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

/*
 * 创建并返回给定字典的不安全迭代器
 *
 * T = O(1)
 */
dictIterator *dictGetIterator(dict *d)
{
    //为迭代器分配空间
    dictIterator *iter = zmalloc(sizeof(*iter));
    //初始化
    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

/*
 * 创建并返回给定节点的安全迭代器
 *
 * T = O(1)
 */
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);
    //安全标志位置为1
    i->safe = 1;
    return i;
}

/*
 * 返回迭代器指向的当前节点
 *
 * 字典迭代完毕时，返回 NULL
 *
 * T = O(1)
 */
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        // 进入这个循环有两种可能：
        // 1) 这是迭代器第一次运行
        // 2) 当前索引链表中的节点已经迭代完（NULL 为链表的表尾）
        if (iter->entry == NULL) {
            // 指向被迭代的哈希表
            dictht *ht = &iter->d->ht[iter->table]; 
            // 初次迭代时执行
            if (iter->index == -1 && iter->table == 0) {
                // 如果是安全迭代器，那么更新安全迭代器计数器
                if (iter->safe)
                    iter->d->iterators++;
                // 如果是不安全迭代器，那么计算指纹
                else
                    iter->fingerprint = dictFingerprint(iter->d); 
            }
            // 更新索引
            iter->index++;
            // 如果迭代器的当前索引大于当前被迭代的哈希表的大小
            // 那么说明这个哈希表已经迭代完毕
            if (iter->index >= (long) ht->size) {
                // 如果正在 rehash 的话，那么说明 1 号哈希表也正在使用中
                // 那么继续对 1 号哈希表进行迭代
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                // 如果没有 rehash ，那么说明迭代已经完成
                } else {
                    break;
                }
            }
            // 如果进行到这里，说明这个哈希表并未迭代完
            // 更新节点指针，指向下个索引链表的表头节点
            iter->entry = ht->table[iter->index];
        } else {
            // 执行到这里，说明程序正在迭代某个链表
            // 将节点指针指向链表的下个节点
            iter->entry = iter->nextEntry;
        }
        // 如果当前节点不为空，那么也记录下该节点的下个节点
        // 因为安全迭代器有可能会将迭代器返回的当前节点删除
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL; //遍历结束
}

/*
 * 释放给定字典迭代器
 *
 * T = O(1)
 */
void dictReleaseIterator(dictIterator *iter)
{
    if (!(iter->index == -1 && iter->table == 0)) {
        // 释放安全迭代器时，安全迭代器计数器减一
        if (iter->safe)
            iter->d->iterators--;
        // 释放不安全迭代器时，验证指纹是否有变化
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
/*
 * 随机返回字典中任意一个节点。
 *
 * 可用于实现随机化算法。
 *
 * 如果字典为空，返回 NULL 。
 *
 * T = O(N)
*/
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned int h;
    int listlen, listele;
    //若hash表为空,返回NULL
    if (dictSize(d) == 0) return NULL;
    //如果正在进行rehash，则执行一次rehash操作
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 如果正在 rehash ，那么将 1 号哈希表也作为随机查找的目标
    if (dictIsRehashing(d)) {
        // T = O(N)
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            h = d->rehashidx + (random() % (d->ht[0].size +
                                            d->ht[1].size -
                                            d->rehashidx));
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    // 否则，只从 0 号哈希表中查找节点
    } else {
        // T = O(N)
        do {
            h = random() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    listlen = 0;
    orighe = he;
    while(he) {
        he = he->next;
        listlen++;
    }
    listele = random() % listlen;
    he = orighe;
    while(listele--) he = he->next;
    return he;
}

/* This function samples the dictionary to return a few keys from random
 * locations.
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 *
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 *
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements. */
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) count = dictSize(d);
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    unsigned long i = random() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    while(stored < count && maxsteps--) {
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size) i = d->rehashidx;
                continue;
            }
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = random() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count) return stored;
                }
            }
        }
        i = (i+1) & maxsizemask;
    }
    return stored;
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v) {
    unsigned long s = 8 * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * dictScan() 函数用于迭代给定字典中的元素。
 *
 * Iterating works the following way:
 *
 * 迭代按以下方式执行：
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 *    一开始，你使用 0 作为游标来调用函数。
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 *    函数执行一步迭代操作，
 *    并返回一个下次迭代时使用的新游标。
 * 3) When the returned cursor is 0, the iteration is complete.
 *    当函数返回的游标为 0 时，迭代完成。
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * 函数保证，在迭代从开始到结束期间，一直存在于字典的元素肯定会被迭代到，
 * 但一个元素可能会被返回多次。
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 *
 * 每当一个元素被返回时，回调函数 fn 就会被执行，
 * fn 函数的第一个参数是 privdata ，而第二个参数则是字典节点 de 。
 *
 * HOW IT WORKS.
 * 工作原理
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * 迭代所使用的算法是由 Pieter Noordhuis 设计的，
 * 算法的主要思路是在二进制高位上对游标进行加法计算
 * 也即是说，不是按正常的办法来对游标进行加法计算，
 * 而是首先将游标的二进制位翻转（reverse）过来，
 * 然后对翻转后的值进行加法计算，
 * 最后再次对加法计算之后的结果进行翻转。
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 *
 * 这一策略是必要的，因为在一次完整的迭代过程中，
 * 哈希表的大小有可能在两次迭代之间发生改变。
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * 哈希表的大小总是 2 的某个次方，并且哈希表使用链表来解决冲突，
 * 因此一个给定元素在一个给定表的位置总可以通过 Hash(key) & SIZE-1
 * 公式来计算得出，
 * 其中 SIZE-1 是哈希表的最大索引值，
 * 这个最大索引值就是哈希表的 mask （掩码）。
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * 举个例子，如果当前哈希表的大小为 16 ，
 * 那么它的掩码就是二进制值 1111 ，
 * 这个哈希表的所有位置都可以使用哈希值的最后四个二进制位来记录。
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 * 如果哈希表的大小改变了怎么办？
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * 当对哈希表进行扩展时，元素可能会从一个槽移动到另一个槽，
 * 举个例子，假设我们刚好迭代至 4 位游标 1100 ，
 * 而哈希表的 mask 为 1111 （哈希表的大小为 16 ）。
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * 如果这时哈希表将大小改为 64 ，那么哈希表的 mask 将变为 111111 ，
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 * 等等。。。在 rehash 的时候可是会出现两个哈希表的阿！
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 * 限制
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 * 这个迭代器是完全无状态的，这是一个巨大的优势，
 * 因为迭代可以在不使用任何额外内存的情况下进行。
 *
 * The disadvantages resulting from this design are:
 * 这个设计的缺陷在于：
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 *    函数可能会返回重复的元素，不过这个问题可以很容易在应用层解决。
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 *    为了不错过任何元素，
 *    迭代器需要返回给定桶上的所有键，
 *    以及因为扩展哈希表而产生出来的新表，
 *    所以迭代器必须在一次迭代中返回多个元素。
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 *    对游标进行翻转（reverse）的原因初看上去比较难以理解，
 *    不过阅读这份注释应该会有所帮助。
 */
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction* bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    // 跳过空字典
    if (dictSize(d) == 0) return 0;

    // 迭代只有一个哈希表的字典
    if (!dictIsRehashing(d)) {
        // 指向哈希表
        t0 = &(d->ht[0]);
        // 记录 mask
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        // 遍历桶中的所有节点
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

    // 迭代有两个哈希表的字典
    } else {
        // 指向两个哈希表
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        // 确保 t0 比 t1 要小
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        // 记录掩码
        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        // Iterate over indices in larger table             // 迭代大表中的桶
        // that are the expansion of the index pointed to   // 这些桶被索引的 expansion 所指向
        // by the cursor in the smaller table               //
        do {
            /* Emit entries at cursor */
            if (bucketfn) bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment bits not covered by the smaller mask */
            v = (((v | m0) + 1) & ~m0) | (v & m0);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    /* Set unmasked bits so incrementing the reversed cursor
     * operates on the masked bits of the smaller table */
    v |= ~m0;

    /* Increment the reverse cursor */
    v = rev(v);
    v++;
    v = rev(v);

    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Expand the hash table if needed */
/*
 * 根据需要，初始化字典（的哈希表），或者对字典（的现有哈希表）进行扩展
 *
 * T = O(N)
 */
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    //若此时正在进行rehash操作，则返回DICT_OK
    // 渐进式 rehash 已经在进行了，直接返回
    if (dictIsRehashing(d)) return DICT_OK;

    /* If the hash table is empty expand it to the initial size. */
    // 如果字典（的 0 号哈希表）为空，那么创建并返回初始化大小的 0 号哈希表
    // T = O(1)
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
     //如果ratio = used / size >= 1, 若字典扩展状态为1或者ratio > dict_force_resize_ratio，则执行扩展操作
    // 以下两个条件之一为真时，对字典进行扩展
    // 1）字典已使用节点数和字典大小之间的比率接近 1：1
    //    并且 dict_can_resize 为真
    // 2）已使用节点数和字典大小之间的比率超过 dict_force_resize_ratio
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        // 新哈希表的大小至少是目前已使用节点数的两倍
        // T = O(N)
        return dictExpand(d, d->ht[0].used*2);
    }
    return DICT_OK;
}

/* Our hash table capability is a power of two */
/*
 * 计算第一个大于等于 size 的 2 的 N 次方，用作哈希表的值
 *
 * T = O(1)
 */
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 * 返回可以将 key 插入到哈希表的索引位置
 * 如果 key 已经存在于哈希表，那么返回 -1
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table.
 //返回key对应字典当中的索引值
 * 注意，如果字典正在进行 rehash ，那么总是返回 1 号哈希表的索引。
 * 因为在字典进行 rehash 时，新节点总是插入到 1 号哈希表。
 *
 * T = O(N)
 */
static int _dictKeyIndex(dict *d, const void *key, unsigned int hash, dictEntry **existing)
{
    unsigned int idx, table;
    dictEntry *he;
    if (existing) *existing = NULL;

    /* Expand the hash table if needed */
    // 单步 rehash
    // T = O(N)
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;
    /* Compute the key hash value */
    // 计算 key 的哈希值
    // T = O(1)
    for (table = 0; table <= 1; table++) {
        // 计算索引值
        idx = hash & d->ht[table].sizemask;
        /* Search if this slot does not already contain the given key */
        // 查找 key 是否存在
        // T = O(1)
        he = d->ht[table].table[idx];
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }
        // 如果运行到这里时，说明 0 号哈希表中所有节点都不包含 key
        // 如果这时 rehahs 正在进行，那么继续对 1 号哈希表进行 rehash
        if (!dictIsRehashing(d)) break;
    }
    // 返回索引值
    return idx;
}

/*
 * 清空字典上的所有哈希表节点，并重置字典属性
 *
 * T = O(N)
 */
void dictEmpty(dict *d, void(callback)(void*)) {
    // 删除两个哈希表上的所有节点
    // T = O(N)
    _dictClear(d,&d->ht[0],callback);
    _dictClear(d,&d->ht[1],callback);
    // 重置属性 
    d->rehashidx = -1;
    d->iterators = 0;
}
//启用字典可扩展空间的功能
/*
 * 开启自动 rehash
 *
 * T = O(1)
 */
void dictEnableResize(void) {
    dict_can_resize = 1;
}
//禁用字典可扩展空间的功能
/*
 * 关闭自动 rehash
 *
 * T = O(1)
 */
void dictDisableResize(void) {
    dict_can_resize = 0;
}
//获取键对应的hash值
unsigned int dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 * oldkey is a dead pointer and should not be accessed.
 * the hash value should be provided using dictGetHash.
 * no string / key comparison is performed.
 * return value is the reference to the dictEntry if found, or NULL if not found. */
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, unsigned int hash) {
    dictEntry *he, **heref;
    unsigned int idx, table;

    if (d->ht[0].used + d->ht[1].used == 0) return NULL; /* dict is empty */
    for (table = 0; table <= 1; table++) {
        idx = hash & d->ht[table].sizemask;
        heref = &d->ht[table].table[idx];
        he = *heref;
        while(he) {
            if (oldptr==he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0) {
        return snprintf(buf,bufsize,
            "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf+l,bufsize-l,
        "Hash table %d stats (%s):\n"
        " table size: %ld\n"
        " number of elements: %ld\n"
        " different slots: %ld\n"
        " max chain length: %ld\n"
        " avg chain length (counted): %.02f\n"
        " avg chain length (computed): %.02f\n"
        " Chain length distribution:\n",
        tableid, (tableid == 0) ? "main hash table" : "rehashing target",
        ht->size, ht->used, slots, maxchainlen,
        (float)totchainlen/slots, (float)ht->used/slots);

    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
            "   %s%ld: %ld (%.02f%%)\n",
            (i == DICT_STATS_VECTLEN-1)?">= ":"",
            i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }

    /* Unlike snprintf(), teturn the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf,bufsize,&d->ht[0],0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf,bufsize,&d->ht[1],1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef DICT_BENCHMARK_MAIN

#include "sds.h"

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0);

/* dict-benchmark [count] */
int main(int argc, char **argv) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType,NULL);
    long count = 0;

    if (argc == 2) {
        count = strtol(argv[1],NULL,10);
    } else {
        count = 5000000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,sdsfromlonglong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        sdsfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        sdsfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        sds key = sdsfromlonglong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
}
#endif
