'use strict';

const DataLoader = require('dataloader');
const shimmer = require('shimmer');

/**
 *
 * @param {object} model
 * @param {string} targetIdAttribute
 * @param {string} relationType
 * @param {?object} queryBuilder
 * @returns {DataLoader}
 */
function modelLoader(model, targetIdAttribute, relationType, queryBuilder) {
    const collection = (relationType === 'hasMany');
    const loader = new DataLoader((keys) => {
        return model.query((db) => {
            Object.assign(db, queryBuilder || {});
            db.where(targetIdAttribute, 'in', keys);
        }).fetchAll().then((items) => {
            const byTargetId = {};
            items.forEach((item) => {
                if (collection) {
                    const key = item.attributes[targetIdAttribute];
                    byTargetId[key] = byTargetId[key] ?
                        byTargetId[key] :
                        [];
                    byTargetId[key].push(item);
                } else byTargetId[item.attributes[targetIdAttribute]] = item;
            });
            return keys.map((key) => {
                if (byTargetId.hasOwnProperty(key)) {
                    return byTargetId[key];
                }
                if (collection === true) {
                    return [];
                }
                return null;
            });
        });
    }, {
        cache: true,
    });
    return loader;
}

/**
 *
 * @param {object} model
 * @param {string} joinTableName
 * @param {string} foreignKey
 * @param {string} otherKey
 * @param {string} targetIdAttribute
 * @param {?object} queryBuilder
 * @returns {DataLoader}
 */
function belongsToManyLoader(model, joinTableName, foreignKey, otherKey, targetIdAttribute, queryBuilder) {
    const loader = new DataLoader((keys) => {
        return model.query((db) => {
            Object.assign(db, queryBuilder || {});
            db.select([
                `${model.prototype.tableName}.*`,
                `${joinTableName}.${foreignKey}`,
                `${joinTableName}.${otherKey}`,
            ]).innerJoin(
                joinTableName, `${model.prototype.tableName}.${targetIdAttribute}`,
                '=',
                `${joinTableName}.${otherKey}`)
                .where(`${joinTableName}.${foreignKey}`, 'in', keys);
        })
            .fetchAll()
            .then((items) => {
                const byForeignKey = {};
                items.forEach((item) => {
                    const key = item.attributes[foreignKey];
                    byForeignKey[key] = byForeignKey[key] ?
                        byForeignKey[key] :
                        [];
                    byForeignKey[key].push(item);
                });
                return keys.map((key) => {
                    if (byForeignKey.hasOwnProperty(key)) {
                        return byForeignKey[key];
                    }
                    return [];
                });
            });
    }, {
        cache: true,
    });
    return loader;
}

/**
 *
 * @param {object} target
 */
function belongsTo(target) {
    if (target.fetch.__wrapped) return;
    shimmer.wrap(target, 'fetch', (original) => {
        return function fetch() {
            const model = this.relatedData.target;
            const targetIdAttribute = this.relatedData.key('targetIdAttribute');
            const parentFK = this.relatedData.key('parentFk');
            const knex = this._knex;
            return (parentFK !== null) ?
                modelLoader(model, targetIdAttribute, 'belongsTo', knex).load(parentFK) :
                Promise.resolve(null);
        };
    });
}

/**
 *
 * @param {object} target
 */
function hasOne(target) {
    if (target.fetch.__wrapped) return;
    shimmer.wrap(target, 'fetch', (original) => {
        return function fetch() {
            const model = this.relatedData.target;
            const foreignKey = this.relatedData.key('foreignKey');
            const parentFK = this.relatedData.key('parentFk');
            const knex = this._knex;
            return modelLoader(model, foreignKey, 'hasOne', knex).load(parentFK);
        };
    });
}

/**
 *
 * @param {object} target
 */
function hasMany(target) {
    if (target.fetch.__wrapped) return;
    shimmer.wrap(target, 'fetch', (original) => {
        return function fetch() {
            const model = this.relatedData.target;
            const foreignKey = this.relatedData.key('foreignKey');
            const parentFK = this.relatedData.key('parentFk');
            const knex = this._knex;
            return modelLoader(model, foreignKey, 'hasMany', knex).load(parentFK);
        };
    });
}

/**
 *
 * @param {object} target
 */
function belongsToMany(target) {
    if (target.fetch.__wrapped) return;
    shimmer.wrap(target, 'fetch', (original) => {
        return function fetch() {
            const model = this.relatedData.target;
            const joinTableName = this.relatedData.key('joinTableName') ||
                [this.tableName(), this.relatedData.key('parentTableName')].sort().join('_');
            const foreignKey = this.relatedData.key('foreignKey');
            const otherKey = this.relatedData.key('otherKey');
            const targetIdAttribute = this.relatedData.key('targetIdAttribute');
            const parentFK = this.relatedData.key('parentFk');
            const knex = this._knex;
            return belongsToManyLoader(model, joinTableName, foreignKey, otherKey, targetIdAttribute, knex)
                .load(parentFK);
        };
    });
}

/**
 *
 * @param {object} target
 */
module.exports = function loaders(target) {
    if (target.relatedData) {
        switch (target.relatedData.type) {
        case 'belongsTo':
            belongsTo(target);
            break;

        case 'belongsToMany':
            belongsToMany(target);
            break;

        case 'hasMany':
            hasMany(target);
            break;

        case 'hasOne':
            hasOne(target);
            break;

        default:
            break;
        }
    }
};
