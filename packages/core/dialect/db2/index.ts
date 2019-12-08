import { ConnectionDialect } from '../../interface';
import * as Utils from '../../utils';
import queries from './queries';
import * as db2Lib from 'ibm_db';
import GenericDialect from '../generic';
import { DatabaseInterface } from '@sqltools/core/plugin-api';

const D2BLibVersion = '2.6.1';
export default class DB2 extends GenericDialect<db2Lib.Database> implements ConnectionDialect {
  public static deps: typeof GenericDialect['deps'] = [{
    type: 'package',
    name: 'ibm_db',
    version: D2BLibVersion
    // args: ['-vscode']
  }];

  queries = queries

  private get lib(): typeof db2Lib.Pool {
    const dbLib = __non_webpack_require__('ibm_db').Pool;
    return dbLib;
  }

  public async open() {
    this.needToInstallDependencies();

    if (this.connection) {
      return this.connection;
    }

    let { connectString } = this.credentials;
    if (!connectString) {
      connectString = `database=${this.credentials.database};`
        + `hostname=${this.credentials.server};`
        + `port=${this.credentials.port};`
        + `uid=${this.credentials.username};`
        + `pwd=${this.credentials.password}`;
    }

    this.connection = this.dbopen(connectString)
    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    await this.dbcloseAll().then(() => {
      this.connection = null;
    })
  }

  private async queryByQuery(
    database: db2Lib.Database, 
    queries: string[],
    results: DatabaseInterface.QueryResults[]
  ): Promise<DatabaseInterface.QueryResults[]> {
    let thiz: DB2 = this;
    if (queries.length==0) {
      database.close();
      return Promise.resolve(results);
    }
    let q = queries[0];
    try {
      if (thiz.isNonQuery(q)) {
        let stmt = database.prepareSync(q);
        return new Promise(function(resolve, reject) {
          stmt.executeNonQuery([], (err: Error, res: any[]) => {
            if (err) {
              database.close();
              reject(err);
            }
            else {
              results.push({
                connId: thiz.getId(),
                cols: [],
                messages: [`${res} rows were affected.`],
                query: q,
                results: [],
              });
              thiz.queryByQuery(database, queries.slice(1), results).then(() => { resolve(results); });
            }
          });
        })
      }
      else {
        let res = thiz.queryResultSync(database, q);
        let row;
        let dataSet = []
        while (row = res.fetchSync()) {
          dataSet.push(row);
        }
        results.push({
          connId: thiz.getId(),
          cols: dataSet && dataSet.length > 0 ? Object.keys(dataSet[0]) : [],
          messages: [],
          query: q,
          results: dataSet,
        });
        return thiz.queryByQuery(database, queries.slice(1), results);
      }
    }
    catch (err) {
      database.close();
      return Promise.reject(err);
    }
  }

  public async query(query: string): Promise<DatabaseInterface.QueryResults[]> {
    let thiz: DB2 = this;
    const database = await thiz.open();
    const queries = Utils.query.parse(query);
    return thiz.queryByQuery(database, queries, []);
  }

  public async testConnection(): Promise<void> {
    return this.query('SELECT 1 FROM SYSIBM.SYSDUMMY1').then(() => void 0);
  }

  public getTables(): Promise<DatabaseInterface.Table[]> {
    return this.query(this.queries.fetchTables)
      .then(([queryRes]) => {
        return queryRes.results
          .reduce((prev, curr) => prev.concat(curr), [])
          .map((obj) => {
            return {
              name: obj.TABLENAME,
              isView: !!obj.ISVIEW,
              numberOfColumns: parseInt(obj.NUMBEROFCOLUMNS, 10),
              tableCatalog: obj.TABLECATALOG,
              tableDatabase: obj.DBNAME,
              tableSchema: obj.TABLESCHEMA,
              tree: obj.TREE,
            } as DatabaseInterface.Table;
          });
      });
  }

  public getColumns(): Promise<DatabaseInterface.TableColumn[]> {
    return this.query(this.queries.fetchColumns)
      .then(([queryRes]) => {
        return queryRes.results
          .reduce((prev, curr) => prev.concat(curr), [])
          .map((obj) => {
            return {
              columnName: obj.COLUMNNAME,
              defaultValue: obj.DEFAULTVALUE,
              isNullable: !!obj.ISNULLABLE ? obj.ISNULLABLE.toString() === 'yes' : null,
              size: obj.Size !== null ? parseInt(obj.Size, 10) : null,
              tableCatalog: obj.TABLECATALOG,
              tableDatabase: obj.DBNAME,
              tableName: obj.TABLENAME,
              tableSchema: obj.TABLESCHEMA,
              type: obj.Type,
              isPk: obj.KEYTYPE === 'P',
              isFk: obj.KEYTYPE === 'R',
              tree: obj.TREE,
            } as DatabaseInterface.TableColumn;
          });
      });
  }

  private getDatabaseName(database: db2Lib.Database): string {
    try {
      let result = this.queryResultSync(database, 'select current_server as NAME from sysibm.sysdummy1');
      let row
      while (row = result.fetchSync()) {
        return row.NAME
      }
    }
    finally {
      if (database) {
        database.close()
      }
    }
    return ''
  }
  public async describeTable(prefixedTable: string): Promise<DatabaseInterface.QueryResults[]> {
    const [schema, table] = prefixedTable.split('.');
    let thiz: DB2 = this;
    const database = await thiz.open();
    return new Promise<DatabaseInterface.QueryResults[]>(
      function (resolve, reject) {
        const results: DatabaseInterface.QueryResults[] = [];
        database.describe({
          database: thiz.getDatabaseName(database),
          schema: schema,
          table: table
        }, (err, res) => {
          if (err) {
            // there is an error during querying
            reject(err)
            return
          }
          else {
            results.push({
              connId: thiz.getId(),
              cols: res && res.length > 0 ? Object.keys(res[0]) : [],
              messages: [],
              query: 'describe',
              results: res,
            });
            resolve(results)
          }
        })
      }
    )
  }

  public getFunctions(): Promise<DatabaseInterface.Function[]> {
    return this.query(this.queries.fetchFunctions)
      .then(([queryRes]) => {
        return queryRes.results
          .reduce((prev, curr) => prev.concat(curr), [])
          .map((obj) => {
            return {
              name: obj.NAME,
              schema: obj.DBSCHEMA,
              database: obj.DBNAME,
              signature: obj.SIGNATURE,
              args: obj.ARGS ? obj.ARGS.split(/, */g) : [],
              resultType: obj.RESULTTYPE,
              tree: obj.TREE,
            } as DatabaseInterface.Function;
          });
      });
  }

  private _pool: db2Lib.Pool
  private gePool(): db2Lib.Pool {
    if (!this._pool) {
      this._pool = new this.lib();
      this._pool.setMaxPoolSize(10);
    }
    return this._pool
  }

  private queryResultSync(database: db2Lib.Database, query: string, params?: any): db2Lib.ODBCResult {
    let result = database.queryResultSync(query, params)
    if (this.hasError(result)) {
      throw this.getError(result)
    }
    return result;
  }
  
  private isNonQuery(query: string): boolean {
    return /^(?:insert|update|merge|delete)/i.test(query);
  }

  private hasError(result: db2Lib.ODBCResult): boolean {
    return !result.fetchMode
  }

  private getError(result: db2Lib.ODBCResult): { error, sqlcode, message, state } {
    return (({ error, sqlcode, message, state }) => ({ error, sqlcode, message, state }))(<any>result)
  }

  private async dbcloseAll(): Promise<void> {
    let pool: db2Lib.Pool = this.gePool()
    return new Promise<void>(
      function (resolve) {
        pool.close(() => {
          resolve();
        })
      });
  }

  private async dbopen(connectString: string): Promise<db2Lib.Database> {
    let pool: db2Lib.Pool = this.gePool()
    return new Promise<db2Lib.Database>(
      function (resolve, reject) {
        pool.open(connectString, function (err, db) {
          if (err) {
            reject(err);
          }
          else {
            resolve(db);
          }
        })
      });
  }

}
