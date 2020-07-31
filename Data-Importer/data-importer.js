const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const ObjectId = Schema.Types.ObjectId;
const ObjectID = require('mongodb').ObjectId;

const ExcelJS = require('exceljs');

const workbook = new ExcelJS.Workbook();

const handleNext = (acc, next) => {
    if(next.length>0){
        next[0](acc, next.slice(1));
    }
    else{
        setTimeout(() => {
            console.log('Upload complete.');
            process.exit(0)
        }, 3000);
    }
}

const Category = mongoose.model('Category', new Schema({
    name: { type: String, required: true },
    COA: String,
    id: { type: String, required: true, unique: true }
}, {minimize: false}), 'Category'
);

const CategoryGroup = mongoose.model('CategoryGroup', new Schema({
    name: { type: String, required: true },
    isActive: Boolean
}, {minimize: false}), 'CategoryGroup'
);

const CategoryTree = mongoose.model('CategoryTree', Schema({
    categoryGroupId: { type: ObjectId, ref: "CategoryGroup" },
    parentId: { type: ObjectId, ref: "CategoryTree" },
    categoryId: [{ type: String }],
    sheetNameId: { type: ObjectId, ref: 'SheetName' }
}, {minimize: false}), 'CategoryTree'
);

const connectToDb = (acc, next) => {
    if(mongoose.connection.readyState==0){
        console.log('Connecting to MongoDB...');
        mongoose.connect(Constants.DB_CONNECTION_STRING, {
            useNewUrlParser: true,
            useCreateIndex: true,
            useFindAndModify: false,
            useUnifiedTopology: true
        }).then(
            res => console.log('Successfully connected to MongoDB.')
        ).catch(
            err => console.log('Failed to connect to MongoDB.')
        );
    }

    Category.find({}).then(db_category => {
        CategoryGroup.find({}).then(db_category_group => {
            CategoryTree.find({}).then(db_category_tree => {
                handleNext(Object.assign({}, acc, { db_category, db_category_group, db_category_tree }), next);
            })
        });
    });
}

class Node{
    constructor(categoryGroup, parent, _id = null){
        this.categoryGroup = categoryGroup;
        this.parent = parent;
        this.children = [];
        this.leaves = [];
        this.categoryId = [];
        this._id = _id;
    }
}

const union = (a, b) => [...new Set([...a, ...b])];

let category = [], categoryTree = [new Node('null', 0)], db_categoryTree = [new Node('null', 0)];
let categoryId = [];
let categoryGroup = {};

const addPath = (n, path) => {
    if(path.length==0) return n;
    for(let v of categoryTree[n].children){
        if(categoryTree[v].categoryGroup===path[0])
            return addPath(v, path.slice(1));
    }
    categoryTree.push(new Node(path[0], n));
    categoryTree[n].children.push(categoryTree.length-1);
    return addPath(categoryTree.length-1, path.slice(1));
}

const validToken = (_token) => {
    const token = _token.trim();
    return (token.length > 0 && token.split(' ').length <= 1);
}

const validSelector = (_token) => {
    const token = _token.trim();
    if(token.match(/(\s|^)\d{1,4}(\s|$)/ig)) return false;
    if(token.match(/\sto\s/ig)){
        const tokens = token.split(/\sto\s/ig);
        if(tokens.length != 2) return false;
        else return validToken(tokens[0])&&validToken(tokens[1]);
    }
    else return validToken(token);
}

const parseCOA = (inp = '') => {
    const REMOVE_WORDS = /(?<![a-zA-Z0-9~\*\&:])[A-Z\.\-:]+(?![a-zA-Z0-9~\*\&:])(?<![^a-zA-Z0-9~\*\&:]TO)/ig;
    const code = (typeof inp == "number")? inp.toString() : inp;
    const segments = code.replace(REMOVE_WORDS, '').split(/[\[\]\(\)\<\>]/);
    let include = [], exclude = [];
    for(let i=0;i<segments.length;i++){
        const block = segments[i];
        if(i&1) exclude = exclude.concat(block.split(/,/g));
        else include = include.concat(block.split(/,/g));
    }
    include = include.filter(block => validSelector(block)).map(block => block.replace(/\s/g, ''));
    exclude = exclude.filter(block => validSelector(block)).map(block => block.replace(/\s/g, ''));
    return {
        include: include.join('_') || '',
        exclude: exclude.join('_') || ''
    };
}

const convertToQuery = (PA, SA) => {
    const {
        include: PA_inc,
        exclude: PA_exc
    } = parseCOA(PA);
    const {
        include: SA_inc,
        exclude: SA_exc
    } = parseCOA(SA);
    if(SA_inc.length>0) return `pa=${PA_inc}` + (PA_exc.length>0? `&exclude=${PA_exc}` : "") + `&sa=${SA_inc}` + (SA_exc.length>0? `&exclude=${SA_exc}` : "");
    else if(PA_inc.length>0) return `pa=${PA_inc}` + (PA_exc.length>0? `&exclude=${PA_exc}` : "");
    else return '';
}

const processWorksheet = worksheet => {
    const _N = worksheet._rows.length;
    let N = 0, M = 0;
    for(let i=1;i<=_N;i++){
        if(worksheet.getRow(i)._cells.length>0) N = i;
        worksheet.getRow(i).eachCell((cell, colNum) => M = (colNum>M)? colNum : M);
    }

    for(let i=1;i<=N;i++){
        const id = worksheet.getRow(i).getCell(Constants.ID).value;
        const path = worksheet.getRow(i).getCell(Constants.GROUPNAME).value;
        const name = worksheet.getRow(i).getCell(Constants.NAME).value;
        if(typeof path == "string" && typeof name == "string" && !isNaN(id) && id && path.length>0 && name.length>0){
            const PA = worksheet.getRow(i).getCell(Constants.PA).value || '';
            const SA = worksheet.getRow(i).getCell(Constants.SA).value || '';

            const tokens = path.split(Constants.SPLIT_BY).map(token => token.trim());
            const par = addPath(0, tokens);

            category.push({ name, id: `${id}`, COA: convertToQuery(PA, SA) });
            categoryTree[par].leaves.push(category.length-1);
        }
    }
}

const mergeTrees = (new_idx, old_idx, path = [], parentId = null) => {
    for(let v of categoryTree[new_idx].leaves)
        categoryTree[new_idx].categoryId.push(categoryId[v]);
    if(old_idx==-1){
        const name = path.join(` ${Constants.SPLIT_BY} `);
        // const name = categoryTree[new_idx].categoryGroup;
        console.log(`creating: ${name}`);
        CategoryGroup.create({ name }).then(res => {
            CategoryTree.create({ 
                parentId,
                categoryId: categoryTree[new_idx].categoryId,
                categoryGroupId: res._id
            }).then(node_res => {
                for(let v of categoryTree[new_idx].children){
                    mergeTrees(v, old_idx, path.concat(categoryTree[v].categoryGroup), node_res._id);
                }
            });
        });
    }
    else{
        const ct_string = categoryTree[new_idx].categoryId;
        const dbct_string = db_categoryTree[old_idx].categoryId;
        categoryTree[new_idx].categoryId=union(ct_string,dbct_string);
        categoryTree[new_idx].categoryGroupId=db_categoryTree[old_idx].categoryGroupId;
        categoryTree[new_idx]._id =db_categoryTree[old_idx]._id;
        for(let v of categoryTree[new_idx].children){
            let fnd = 0;
            for(let u of db_categoryTree[old_idx].children){
                const child_path = path.concat(categoryTree[v].categoryGroup).join(' - '); 
                // const child_path = categoryTree[v].categoryGroup;
                if(child_path===db_categoryTree[u].categoryGroup){
                    mergeTrees(v, u, path.concat(categoryTree[v].categoryGroup), categoryTree[new_idx]._id);
                    fnd = 1;
                    break;
                }
            }
            if(!fnd) mergeTrees(v, -1, path.concat(categoryTree[v].categoryGroup), categoryTree[new_idx]._id);
        }
        if(new_idx>0){
            console.log(`updating: ${path.join(` ${Constants.SPLIT_BY} `)}`);
            CategoryTree.findByIdAndUpdate(categoryTree[new_idx]._id, {
                parentId,
                categoryId: categoryTree[new_idx].categoryId,
                categoryGroupId: categoryTree[new_idx].categoryGroupId
            }, (res, err) => {});
        }
    }
}

const processTrees = (acc, next) => {
    const { db_category_group, db_category_tree } = acc;
    db_category_group.forEach(group => categoryGroup[group._id]=group.name);
    let map = { null: 0 };
    for(let i=0;i<db_category_tree.length;i++){
        map[db_category_tree[i]._id]=i+1;
    }

    for(let v of db_category_tree){
        const parent = map[v.parentId];
        db_categoryTree.push(new Node(categoryGroup[v.categoryGroupId], parent, v._id));
        db_categoryTree[db_categoryTree.length-1].categoryId = v.categoryId;
        db_categoryTree[db_categoryTree.length-1].categoryGroupId = v.categoryGroupId;
    }
    for(let i=0;i<db_category_tree.length;i++){
        let parent = map[db_category_tree[i].parentId];
        if(parent===undefined) parent=0; // parent does not exist, set it to null to avoid crashing
        db_categoryTree[parent].children.push(i+1);
    }
    mergeTrees(0, 0);
    handleNext(acc, next);
}

let db_keys = {};

const uploadDocument = (index, objects, preferredKey, next) => {
    if(index==objects.length) next();
    else{
        if(objects[index].hasOwnProperty('new')){
            delete objects[index].new;
            Category.create(objects[index]).then(res => {
                categoryId[index] = objects[index].id;
                uploadDocument(index+1, objects, preferredKey, next);
            })
        }
        else{
            const id = db_keys[objects[index][preferredKey]];
            categoryId[index] = objects[index][preferredKey];
            Category.findByIdAndUpdate(id, objects[index], res => {
                uploadDocument(index+1, objects, preferredKey, next);
            })
        }
    }
}

const processDocuments = (acc, next) => {
    const { db_category } = acc;
    const preferredKey = 'id';

    db_keys = db_category.reduce((acc, cur) => Object.assign({}, acc, { [cur[preferredKey]]: cur._id }), {});
    while(categoryId.length<category.length) categoryId.push(null);
    
    const mod_objects = category.map(obj => !db_keys.hasOwnProperty(obj[preferredKey])? {...obj, new: true} : obj);

    const created = mod_objects.filter(obj => obj.hasOwnProperty('new')).length;
    const updated = mod_objects.length-created;

    const report = () => {
        console.log(`Created ${created} categories.`);
        console.log(`Updated ${updated} categories.`);
    }

    const gotoNext = () => {
        report();
        handleNext(acc, next);
    }
    uploadDocument(0, mod_objects, preferredKey, gotoNext);
}

const colToInt = (col) => {
    let res = 0, base = 1;
    while(col.length>0){
        res += (col.charCodeAt(col.length-1)-'A'.charCodeAt(0)+1)*base;
        base *= 26;
        col = col.substring(0, col.length-1);
    }
    return res;
}

const processExcel = (acc, next) => {
    const { fileName } = acc;
    console.log('Opening Excel file...');
    workbook.xlsx.readFile(fileName).then(
        workbook => {
            console.log('Opened Excel file.');
            for(let index=1;index<=workbook._worksheets.length-1;index++){
                const worksheet = workbook._worksheets[index];
                if(parseSheetNames.includes(worksheet.name)){
                    processWorksheet(worksheet);
                }
            }
            
            handleNext(acc, next);
        }
    ).catch(err => console.log('Failed to open Excel file.'));
}

const importExcel = fileName => {
    const processes = [connectToDb, processExcel, processDocuments, processTrees];
    handleNext({ fileName }, processes);
}

const parseSheetNames = [
    'R&E',
    'BalanceSheet',
    'Balance Sheet'
];

// const Constants = {
//     DB_CONNECTION_STRING: "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false",
//     ID: colToInt('A'),
//     GROUPNAME: colToInt('E'),
//     NAME: colToInt('F'),
//     PA: colToInt('Q'),
//     SA: colToInt('S'),
//     SPLIT_BY: '-',
// };

const Constants = {
    DB_CONNECTION_STRING: "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false",
    ID: colToInt('A'),
    GROUPNAME: colToInt('B'),
    NAME: colToInt('C'),
    PA: colToInt('M'),
    SA: colToInt('N'),
    SPLIT_BY: '-',
};

importExcel('./coa_upload.xlsx');