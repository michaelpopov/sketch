#include "lmdb.h"
#include "lmdb2.h"
#include "shared_types.h"
#include "log.h"
#include "gtest/gtest.h"

#include <experimental/scope>
#include <filesystem>
#include <thread>

#include <sys/stat.h>

using namespace sketch;

#if LMDB_TEST

#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

TEST(LMDB, Example_1) {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor, *cur2;
	MDB_cursor_op op;
	int count;
	int *values;
	char sval[32] = "";

	srand(time(NULL));

	    count = (rand()%384) + 64;
	    values = (int *)malloc(count*sizeof(int));

	    for(i = 0;i<count;i++) {
			values[i] = rand()%1024;
	    }
    
        const char* dir = "/tmp/lmdb_test";
        mkdir(dir, 0755);
        std::experimental::scope_exit dir_deleter([&] {
            std::filesystem::remove_all(dir);
        });

		ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
		ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxreaders(env, 1));
		ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
		ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));

		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, NULL, 0, &dbi));
   
		key.mv_size = sizeof(int);
		key.mv_data = sval;

		////printf("Adding %d values\n", count);
	    for (i=0;i<count;i++) {	
			sprintf(sval, "%03x %d foo bar", values[i], values[i]);
			/* Set <data> in each iteration, since MDB_NOOVERWRITE may modify it */
			data.mv_size = sizeof(sval);
			data.mv_data = sval;
			if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE))) {
				j++;
				data.mv_size = sizeof(sval);
				data.mv_data = sval;
			}
	    }
		//if (j) //printf("%d duplicates skipped\n", j);
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));

		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
		while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
			/*//printf("key: %p %.*s, data: %p %.*s\n",
				key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
				data.mv_data, (int) data.mv_size, (char *) data.mv_data);*/
		}
		CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
		mdb_cursor_close(cursor);
		mdb_txn_abort(txn);

		j=0;
		key.mv_data = sval;
	    for (i= count - 1; i > -1; i-= (rand()%5)) {
			j++;
			txn=NULL;
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
			sprintf(sval, "%03x ", values[i]);
			if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, NULL))) {
				j--;
				mdb_txn_abort(txn);
			} else {
				ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
			}
	    }
	    free(values);
		//printf("Deleted %d values\n", j);

		ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
		//printf("Cursor next\n");
		while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
			//printf("key: %.*s, data: %.*s\n",
			//	(int) key.mv_size,  (char *) key.mv_data,
			//	(int) data.mv_size, (char *) data.mv_data);
		}
		CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
		//printf("Cursor last\n");
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_get(cursor, &key, &data, MDB_LAST));
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
		//printf("Cursor prev\n");
		while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
			//printf("key: %.*s, data: %.*s\n",
			//	(int) key.mv_size,  (char *) key.mv_data,
			//	(int) data.mv_size, (char *) data.mv_data);
		}
		CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
		//printf("Cursor last/prev\n");
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_get(cursor, &key, &data, MDB_LAST));
			//printf("key: %.*s, data: %.*s\n",
			//	(int) key.mv_size,  (char *) key.mv_data,
			//	(int) data.mv_size, (char *) data.mv_data);
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_get(cursor, &key, &data, MDB_PREV));
			//printf("key: %.*s, data: %.*s\n",
			//	(int) key.mv_size,  (char *) key.mv_data,
			//	(int) data.mv_size, (char *) data.mv_data);

		mdb_cursor_close(cursor);
		mdb_txn_abort(txn);

		//printf("Deleting with cursor\n");
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cur2));
		for (i=0; i<50; i++) {
			if (RES(MDB_NOTFOUND, mdb_cursor_get(cur2, &key, &data, MDB_NEXT)))
				break;
			//printf("key: %p %.*s, data: %p %.*s\n",
			//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
			//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
			ASSERT_EQ(MDB_SUCCESS, mdb_del(txn, dbi, &key, NULL));
		}

		//printf("Restarting cursor in txn\n");
		for (op=MDB_FIRST, i=0; i<=32; op=MDB_NEXT, i++) {
			if (RES(MDB_NOTFOUND, mdb_cursor_get(cur2, &key, &data, op)))
				break;
			//printf("key: %p %.*s, data: %p %.*s\n",
			//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
			//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
		}
		mdb_cursor_close(cur2);
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));

		//printf("Restarting cursor outside txn\n");
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
		for (op=MDB_FIRST, i=0; i<=32; op=MDB_NEXT, i++) {
			if (RES(MDB_NOTFOUND, mdb_cursor_get(cursor, &key, &data, op)))
				break;
			//printf("key: %p %.*s, data: %p %.*s\n",
			//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
			//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
		}
		mdb_cursor_close(cursor);
		mdb_txn_abort(txn);

		mdb_dbi_close(env, dbi);
		mdb_env_close(env);
}

TEST(LMDB, Example_2) {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	int count;
	int *values;
	char sval[32] = "";

	srand(time(NULL));

	count = (rand()%384) + 64;
	values = (int *)malloc(count*sizeof(int));

	for(i = 0;i<count;i++) {
		values[i] = rand()%1024;
	}

    const char* dir = "/tmp/lmdb_test";
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

	ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxreaders(env, 1));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxdbs(env, 4));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP|MDB_NOSYNC, 0664));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, "id1", MDB_CREATE, &dbi));
   
	key.mv_size = sizeof(int);
	key.mv_data = sval;

	//printf("Adding %d values\n", count);
	for (i=0;i<count;i++) {	
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		data.mv_size = sizeof(sval);
		data.mv_data = sval;
		if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE)))
			j++;
	}
	//if (j) printf("%d duplicates skipped\n", j);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %p %.*s, data: %p %.*s\n",
		//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
		//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	j=0;
	key.mv_data = sval;
	for (i= count - 1; i > -1; i-= (rand()%5)) {
		j++;
		txn=NULL;
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(sval, "%03x ", values[i]);
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, NULL))) {
			j--;
			mdb_txn_abort(txn);
		} else {
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		}
	}
	free(values);
	//printf("Deleted %d values\n", j);

	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	//printf("Cursor next\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	//printf("Cursor prev\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	mdb_env_close(env);
}

TEST(LMDB, Example_3) {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	int count;
	int *values;
	char sval[32];
	char kval[sizeof(int)];

	srand(time(NULL));

	memset(sval, 0, sizeof(sval));

	count = (rand()%384) + 64;
	values = (int *)malloc(count*sizeof(int));

	for(i = 0;i<count;i++) {
		values[i] = rand()%1024;
	}

    const char* dir = "/tmp/lmdb_test";
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

    ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxdbs(env, 4));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP|MDB_NOSYNC, 0664));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, "id2", MDB_CREATE|MDB_DUPSORT, &dbi));

	key.mv_size = sizeof(int);
	key.mv_data = kval;
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	//printf("Adding %d values\n", count);
	for (i=0;i<count;i++) {
		if (!(i & 0x0f))
			sprintf(kval, "%03x", values[i]);
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NODUPDATA)))
			j++;
	}
	//if (j) printf("%d duplicates skipped\n", j);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %p %.*s, data: %p %.*s\n",
		//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
		//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	j=0;

	for (i= count - 1; i > -1; i-= (rand()%5)) {
		j++;
		txn=NULL;
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(kval, "%03x", values[i & ~0x0f]);
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		key.mv_size = sizeof(int);
		key.mv_data = kval;
		data.mv_size = sizeof(sval);
		data.mv_data = sval;
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, &data))) {
			j--;
			mdb_txn_abort(txn);
		} else {
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		}
	}
	free(values);
	//printf("Deleted %d values\n", j);

	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	//printf("Cursor next\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	//printf("Cursor prev\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	mdb_env_close(env);
}

TEST(LMDB, Example_4) {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	int count;
	int *values;
	char sval[8];
	char kval[sizeof(int)];

	memset(sval, 0, sizeof(sval));

	count = 510;
	values = (int *)malloc(count*sizeof(int));

	for(i = 0;i<count;i++) {
		values[i] = i*5;
	}

    const char* dir = "/tmp/lmdb_test";
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

	ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxdbs(env, 4));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP|MDB_NOSYNC, 0664));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, "id4", MDB_CREATE|MDB_DUPSORT|MDB_DUPFIXED, &dbi));

	key.mv_size = sizeof(int);
	key.mv_data = kval;
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	//printf("Adding %d values\n", count);
	strcpy(kval, "001");
	for (i=0;i<count;i++) {
		sprintf(sval, "%07x", values[i]);
		if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NODUPDATA)))
			j++;
	}
	//if (j) printf("%d duplicates skipped\n", j);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));

	/* there should be one full page of dups now.
	 */
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %p %.*s, data: %p %.*s\n",
		//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
		//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	/* test all 3 branches of split code:
	 * 1: new key in lower half
	 * 2: new key at split point
	 * 3: new key in upper half
	 */

	key.mv_size = sizeof(int);
	key.mv_data = kval;
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	sprintf(sval, "%07x", values[3]+1);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	(void)RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NODUPDATA));
	mdb_txn_abort(txn);

	sprintf(sval, "%07x", values[255]+1);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	(void)RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NODUPDATA));
	mdb_txn_abort(txn);

	sprintf(sval, "%07x", values[500]+1);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	(void)RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NODUPDATA));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));

	/* Try MDB_NEXT_MULTIPLE */
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_MULTIPLE)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);
	j=0;

	for (i= count - 1; i > -1; i-= (rand()%3)) {
		j++;
		txn=NULL;
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(sval, "%07x", values[i]);
		key.mv_size = sizeof(int);
		key.mv_data = kval;
		data.mv_size = sizeof(sval);
		data.mv_data = sval;
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, &data))) {
			j--;
			mdb_txn_abort(txn);
		} else {
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		}
	}
	free(values);
	//printf("Deleted %d values\n", j);

	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	//printf("Cursor next\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	//printf("Cursor prev\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	mdb_env_close(env);
}

TEST(LMDB, Example_5) {
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	int count;
	int *values;
	char sval[32];
	char kval[sizeof(int)];

	srand(time(NULL));

	memset(sval, 0, sizeof(sval));

	count = (rand()%384) + 64;
	values = (int *)malloc(count*sizeof(int));

	for(i = 0;i<count;i++) {
		values[i] = rand()%1024;
	}

    const char* dir = "/tmp/lmdb_test";
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

	ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxdbs(env, 4));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP|MDB_NOSYNC, 0664));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, "id2", MDB_CREATE|MDB_DUPSORT, &dbi));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));

	key.mv_size = sizeof(int);
	key.mv_data = kval;
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	//printf("Adding %d values\n", count);
	for (i=0;i<count;i++) {
		if (!(i & 0x0f))
			sprintf(kval, "%03x", values[i]);
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		if (RES(MDB_KEYEXIST, mdb_cursor_put(cursor, &key, &data, MDB_NODUPDATA)))
			j++;
	}
	//if (j) printf("%d duplicates skipped\n", j);
	mdb_cursor_close(cursor);
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %p %.*s, data: %p %.*s\n",
		//	key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
		//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	j=0;

	for (i= count - 1; i > -1; i-= (rand()%5)) {
		j++;
		txn=NULL;
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(kval, "%03x", values[i & ~0x0f]);
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		key.mv_size = sizeof(int);
		key.mv_data = kval;
		data.mv_size = sizeof(sval);
		data.mv_data = sval;
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, &data))) {
			j--;
			mdb_txn_abort(txn);
		} else {
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		}
	}
	free(values);
	//printf("Deleted %d values\n", j);

	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	//printf("Cursor next\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	//printf("Cursor prev\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	mdb_env_close(env);
}

TEST(LMDB, Example_6) {
	int i = 0, /*j = 0,*/ rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data, sdata;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	//int count;
	//int *values;
	long kval;
	char *sval;
    //char dkbuf[1024];

	srand(time(NULL));

    const char* dir = "/tmp/lmdb_test";
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

	ASSERT_EQ(MDB_SUCCESS, mdb_env_create(&env));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_mapsize(env, 10485760));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_set_maxdbs(env, 4));
	ASSERT_EQ(MDB_SUCCESS, mdb_env_open(env, dir, MDB_FIXEDMAP|MDB_NOSYNC, 0664));

	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_dbi_open(txn, "id6", MDB_CREATE|MDB_INTEGERKEY, &dbi));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	ASSERT_EQ(MDB_SUCCESS, mdb_stat(txn, dbi, &mst));

	sval = (char*)calloc(1, mst.ms_psize / 4);
	key.mv_size = sizeof(long);
	key.mv_data = &kval;
	sdata.mv_size = mst.ms_psize / 4 - 30;
	sdata.mv_data = sval;

	//printf("Adding 12 values, should yield 3 splits\n");
	for (i=0;i<12;i++) {
		kval = i*5;
		sprintf(sval, "%08x", (uint32_t)kval);
		data = sdata;
		(void)RES(MDB_KEYEXIST, mdb_cursor_put(cursor, &key, &data, MDB_NOOVERWRITE));
	}
	//printf("Adding 12 more values, should yield 3 splits\n");
	for (i=0;i<12;i++) {
		kval = i*5+4;
		sprintf(sval, "%08x", (uint32_t)kval);
		data = sdata;
		(void)RES(MDB_KEYEXIST, mdb_cursor_put(cursor, &key, &data, MDB_NOOVERWRITE));
	}
	//printf("Adding 12 more values, should yield 3 splits\n");
	for (i=0;i<12;i++) {
		kval = i*5+1;
		sprintf(sval, "%08x", (uint32_t)kval);
		data = sdata;
		(void)RES(MDB_KEYEXIST, mdb_cursor_put(cursor, &key, &data, MDB_NOOVERWRITE));
	}
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_get(cursor, &key, &data, MDB_FIRST));

	do {
		//printf("key: %p %s, data: %p %.*s\n",
		//	key.mv_data,  mdb_dkey(&key, dkbuf),
		//	data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	} while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0);
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_commit(txn);

#if 0
	j=0;

	for (i= count - 1; i > -1; i-= (rand()%5)) {
		j++;
		txn=NULL;
		ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(kval, "%03x", values[i & ~0x0f]);
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		key.mv_size = sizeof(int);
		key.mv_data = kval;
		data.mv_size = sizeof(sval);
		data.mv_data = sval;
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, &data))) {
			j--;
			mdb_txn_abort(txn);
		} else {
			ASSERT_EQ(MDB_SUCCESS, mdb_txn_commit(txn));
		}
	}
	free(values);
	//printf("Deleted %d values\n", j);

	ASSERT_EQ(MDB_SUCCESS, mdb_env_stat(env, &mst));
	ASSERT_EQ(MDB_SUCCESS, mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	ASSERT_EQ(MDB_SUCCESS, mdb_cursor_open(txn, dbi, &cursor));
	//printf("Cursor next\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	//printf("Cursor prev\n");
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		//printf("key: %.*s, data: %.*s\n",
		//	(int) key.mv_size,  (char *) key.mv_data,
		//	(int) data.mv_size, (char *) data.mv_data);
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
#endif
	mdb_env_close(env);
}
#endif // LMDB_TEST

TEST(LMDB, Lmdb) {
    TempLogLevel temp_level(LL_DEBUG);

    const char* dir = "/tmp/lmdb_ex_test";
    std::filesystem::remove_all(dir);
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

    LmdbEnv lmdb_env(dir);
    ASSERT_EQ(0, lmdb_env.init());

    int iret = lmdb_env.create_db();
    ASSERT_EQ(0, iret);

    {
        auto writer = lmdb_env.open_db(LmdbMode::Write);
        ASSERT_TRUE(writer);

        uint32_t record_id = 0;
        for (uint64_t tag = 1; tag <= 12; tag++, record_id++) {
            iret = writer->write_record(tag, record_id);
            ASSERT_EQ(0, iret);
        }

        iret = writer->commit();
        ASSERT_EQ(0, iret);
    }

    {
        auto reader = lmdb_env.open_db(LmdbMode::Read);
        ASSERT_TRUE(reader);

        uint32_t record_id_check = 0;
        for (uint64_t tag = 1; tag <= 12; tag++, record_id_check++) {
            uint32_t record_id = 0;
            uint16_t cluster_id = 0;
            
            iret = reader->read_record(tag, record_id, cluster_id);
            ASSERT_EQ(0, iret);
            ASSERT_EQ(record_id_check, record_id);
            ASSERT_EQ(InvalidClusterId, cluster_id);
        }
    }
}

TEST(LMDB, LmdbExt) {
    TempLogLevel temp_level(LL_DEBUG);

    const char* dir = "/tmp/lmdb_ex_test";
    std::filesystem::remove_all(dir);
    mkdir(dir, 0755);
    std::experimental::scope_exit dir_deleter([&] {
        std::filesystem::remove_all(dir);
    });

    LmdbEnv lmdb_env(dir);
    ASSERT_EQ(0, lmdb_env.init());

    int iret = lmdb_env.create_db();
    ASSERT_EQ(0, iret);

    {
        auto writer = lmdb_env.open_db(LmdbMode::Write);
        ASSERT_TRUE(writer);

        uint32_t record_id = 0;
        for (uint64_t tag = 1; tag <= 12; tag++, record_id++) {
            uint16_t cluster_id = (uint16_t)(tag <= 6 ? 0 : 1);
            iret = writer->write_record(tag, record_id, cluster_id);
            ASSERT_EQ(0, iret);
        }

        iret = writer->commit();
        ASSERT_EQ(0, iret);
    }

    {
        auto reader = lmdb_env.open_db(LmdbMode::Read);
        ASSERT_TRUE(reader);

        uint32_t record_id_check = 0;
        for (uint64_t tag = 1; tag <= 12; tag++, record_id_check++) {
            uint32_t record_id = 0;
            uint16_t cluster_id = 0;
            uint16_t cluster_id_check = tag <= 6 ? 0 : 1;
            
            iret = reader->read_record(tag, record_id, cluster_id);
            ASSERT_EQ(0, iret);
            ASSERT_EQ(record_id_check, record_id);
            ASSERT_EQ(cluster_id_check, cluster_id);
        }
    }

    {
        auto reader = lmdb_env.open_db(LmdbMode::Read);
        ASSERT_TRUE(reader);

        uint32_t record_id_check = 0;
        for (uint16_t cluster_id = 0; cluster_id <= 1; cluster_id++) {
            iret = reader->open_cursor(cluster_id);
            ASSERT_EQ(0, iret);

            size_t count = 0;
            uint32_t record_id = 0;
            while (0 == reader->next(record_id)) {
                ASSERT_EQ(record_id_check, record_id);
                record_id_check++;
                count++;
            }
            ASSERT_EQ(6, count);
            ASSERT_EQ(MDB_NOTFOUND, reader->next(record_id));
            reader->close_cursor();
        }
    }
}