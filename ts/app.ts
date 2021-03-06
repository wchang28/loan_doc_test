import * as AWS from "aws-sdk";
import * as fs from "fs";
import {Readable} from "stream";
import * as _ from "lodash";
import * as xmldom from "xmldom";
let DOMParser = xmldom.DOMParser;
let XMLSerializer = xmldom.XMLSerializer;
let xpath = require('xpath');
let alasql = require('alasql');

AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: "default"});

export interface Bound {
    t?: number;  // top
    b?: number;  // bottom
    l?: number;  // left
    r?: number;  // right
}

interface LineNodeInfo {
    node: Element;
    bound: Bound;
}

export interface WordInfo extends Bound {
    pg?: number; // page number
    ln?: number; // line number
    v?: string;  // value
    idx?: number;   // word index in the line
}

export interface LineInfo extends Bound {
    pg?: number; // page number
    nb?: boolean
    ln?: number; // line number
    v?: string; // value
    wds?: number;    // number of words
}

export interface PageInfo extends Bound {
    pg?: number; // page number
    lns?: number; // number of line in the page
    wds?: number;   // number of words in the page
}

export interface PageExtraction {
    pi?: PageInfo
    lines?: LineInfo[]
    words?: WordInfo[];
}

export interface AbsoluteWordInfo extends WordInfo {
    aln?: number;   // absolute line number accross pages
    at?: number; // absolute top accross pages
    ab?: number; // absolute bottom accross pages
}

export interface AbsoluteLineInfo extends LineInfo {
    aln?: number;   // absolute line number accross pages
    at?: number; // absolute top accross pages
    ab?: number; // absolute bottom accross pages
}

export interface AbsolutePageInfo extends PageInfo {
    at?: number; // absolute top accross pages
    ab?: number; // absolute bottom accross pages
}

export interface DocumentInfo {
    pgs?: number;    // total number of pages
    lns?: number;    // total number of lines
    wds?: number;   // total number of words
    l?: number;  // left
    r?: number; // right
    at?: number; // absolute top accross pages
    ab?: number; // absolute bottom accross pages
}

export interface DocumentExtraction {
    doc?: DocumentInfo;
    pages?: AbsolutePageInfo[];
    lines?: AbsoluteLineInfo[];
    words?: AbsoluteWordInfo[];
}

function getBoundFromNode(node: Element) : Bound {
    let top = parseInt(node.getAttribute('t'));
    let bottom = parseInt(node.getAttribute('b'));
    let left = parseInt(node.getAttribute('l'));
    let right = parseInt(node.getAttribute('r'));
    return {t: top, b: bottom, l: left, r: right};
}

function processPage(page: number, doc: Document) : PageExtraction {
    let select = xpath.useNamespaces({"a": "http://www.scansoft.com/omnipage/xml/ssdoc-schema3.xsd"});
    let nl = select('//a:ln', doc);
    
    let midPoints:{[midPoint: string]: boolean} = {};
    let lineNodeInfos: {[key: string]: LineNodeInfo} = {};
    for (let i = 0; i < nl.length; i++) {	// for each line
        let lineNode: Element = nl[i];
        let bound = getBoundFromNode(lineNode);
        let midPoint = Math.round(parseFloat((bound.t + bound.b).toString())/2.0);
        midPoints[midPoint.toString()] = true;
        let key = JSON.stringify(bound);
        lineNodeInfos[key] = {node: nl[i], bound: bound};
    }
    let count = 0;
    let mids: number[] = [];
    for (let midPoint in midPoints)
        mids.push(parseInt(midPoint));
    //console.log(mids.length);
    mids.sort((a, b) => {return a-b;});
    //console.log(mids);
    let pageLines: LineInfo[] = [];
    let pageWords: WordInfo[] = [];
    for (let i in mids) {	// for each logical line
        let line_no = parseInt(i);
        let midPoint = mids[i];
        //console.log('I am here 1, mid = ' + midPoint);
        let tempLines:LineNodeInfo[] = [];
        for (let key in lineNodeInfos) {
            let lineInfo = lineNodeInfos[key];
            let bound = lineInfo.bound;
            if ((bound.t - 5) <= midPoint && midPoint <= (bound.b + 5)) {
                tempLines.push(lineInfo);
            }
        }
        // sort the lines by their left position in ascending order
        tempLines.sort((a, b) => {
            return (a.bound.l - a.bound.l);
        });
        
        //console.log('I am here 2: ' + tempLines.length);
        let lineWords: WordInfo[] = [];	// words for the logical line
        let wordsInLogicalLine: string[] = [];
        for (let j in tempLines) {	// for each line in the logical line group
            let lineInfo = tempLines[j];
            let lineNode = lineInfo.node;
            let wordNodes = select('a:wd', lineNode);
            let words: string[] = [];
            let words_: WordInfo[] = [];
            for (let k = 0; k < wordNodes.length; k++) {	// for each word in the line
                let wordNode:Element = wordNodes[k];
                let bound = getBoundFromNode(wordNode);
                let word = wordNode.textContent.trim().replace(/\s+/g, "");
                words.push(word);
                words_.push({pg: page, ln: line_no, v: word, l: bound.l, r: bound.r, t: bound.t, b: bound.b});
            }
            wordsInLogicalLine = wordsInLogicalLine.concat(words);
            lineWords = lineWords.concat(words_);
            let key = JSON.stringify(lineInfo.bound);
            delete lineNodeInfos[key];
        }
        for (let j in lineWords)	// for each word
            lineWords[j].idx = parseInt(j);
            
        pageWords = pageWords.concat(lineWords);
        let line: LineInfo = {pg: page, ln: line_no, v: wordsInLogicalLine.join(' '), wds: wordsInLogicalLine.length};
        let lineBounds: Bound = alasql('SELECT min(l) as l, max(r) as r, min(t) as t, max(b) as b from ?',[lineWords])[0];
        //console.log(JSON.stringify(lineBounds));
        let noBound = (JSON.stringify(lineBounds) === '{}');
        line.nb = noBound;
        line.t = (noBound ? null : lineBounds.t);
        line.b = (noBound ? null : lineBounds.b);
        line.l = (noBound ? null : lineBounds.l);
        line.r = (noBound ? null : lineBounds.r);
        pageLines.push(line);
    }
    
    //console.log(JSON.stringify(pageLines));
    let ret = alasql('SELECT min(l) as l, max(r) as r, min(t) as t, max(b) as b from ? where nb=false',[pageLines]);
    let pi: PageInfo = _.assignIn({pg: page, lns: pageLines.length, wds: pageWords.length}, ret[0]);
    //console.log('page ' + page + ': ' + JSON.stringify(pi));
    return {pi, lines: pageLines, words: pageWords};
}

function aggregatePageExtractions(pageExtractions: PageExtraction[]) : DocumentExtraction {
    let documentExtraction: DocumentExtraction = {
        doc: null
        ,pages: []
        ,lines: []
        ,words: []
    }
    let topOffset = 0;
    let lineOffset = 0;
    for (let i in pageExtractions) {	// for each page
        //console.log([topOffset, lineOffset]);
        let page = parseInt(i) + 1;
        let pageExtraction = pageExtractions[i];
        let pi = pageExtraction.pi;
        let shiftedPage: AbsolutePageInfo = _.assignIn({}, pi, {at: pi.t + topOffset, ab: pi.b + topOffset});
        documentExtraction.pages.push(shiftedPage);
        let shiftedWords: AbsoluteWordInfo[] = alasql('SELECT pg, ln, idx, v, l, r, t, b, ln+' + lineOffset + ' as aln, if(t, t+' + topOffset + ', null) as at, if(b, b+' + topOffset + ', null) as ab from ? ', [pageExtraction.words]);
        documentExtraction.words = documentExtraction.words.concat(shiftedWords);
        let shiftedLines: AbsoluteLineInfo[] = alasql('SELECT pg, ln, v, wds, l, r, t, b, ln+' + lineOffset + ' as aln, if(t, t+' + topOffset + ', null) as at, if(b, b+' + topOffset + ', null) as ab, nb from ? ', [pageExtraction.lines]);
        documentExtraction.lines = documentExtraction.lines.concat(shiftedLines);
        //console.log('page ' + page + ': ' + JSON.stringify(newLines));
        topOffset += (pageExtraction.pi.b + 100);
        lineOffset += pageExtraction.lines.length;
    }
    let ret = alasql('select sum(lns) as lns, sum(wds) as wds, min(l) as l, max(r) as r, min(at) as at, max(ab) as ab from ?', [documentExtraction.pages]);
    documentExtraction.doc = _.assignIn({pgs: documentExtraction.pages.length}, ret[0]);
    return documentExtraction;
}

function getPageXMLDocFromS3(Bucket: string, Key: string) : Promise<Document> {
    let s3 = new AWS.S3();
    return s3.getObject({Bucket, Key}).promise()
    .then((output: AWS.S3.GetObjectOutput) => {
        let buff = <Buffer>output.Body;
        let s = buff.toString("utf16le");   // xml file is encoded with UCS-2 LE BOM (UTF-16 LE) acording to Notepad++
        s = s.substr(1, s.length-1);    // Get rid of BOM in the file
        let doc = new DOMParser().parseFromString(s);
        return doc;
    });   
}

let getPageExtractionFromS3 = (Bucket: string, Key: string, page: number) : Promise<PageExtraction> => (getPageXMLDocFromS3(Bucket, Key).then((doc: Document) => processPage(page, doc)));

let pad_4 = (page: number) : string => ((page < 10 ? "000" : (page < 100 ? "00" : (page < 1000 ? "0" : ""))) + page.toString());
let getPageXMLS3Key = (JobId: string, LoanId: string, page: number) : string => (JobId + "/" + LoanId + "/TXT/page_" + pad_4(page) + ".xml");

let Bucket = "harvest-split";
let JobId = "162de65ed655c5a7328b535c7a716994";
let LoanId = "fdd1f16370c53f777a83df5320b6a899";

getPageExtractionFromS3(Bucket, getPageXMLS3Key(JobId, LoanId, 1), 1)
.then((value: PageExtraction) => {
    console.log(JSON.stringify(value, null, 2));
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
});

/*
let pages = 21;
let promises: Promise<PageExtraction>[] = [];
for (let i = 0; i < pages; i++) {
    let page = i + 1;
    let Key = getPageXMLS3Key(JobId, LoanId, page);
    promises.push(getPageExtractionFromS3(Bucket, Key, page));
}
let p = Promise.all(promises);
p.then((value: PageExtraction[]) => {
    //console.log(value.length);
    return aggregatePageExtractions(value);
}).then((value: DocumentExtraction) => {
    //console.log(JSON.stringify(value, null, 2));
    //console.log("pgs=" + value.pages.length);
    //console.log("lns=" + value.lines.length);
    //console.log("wds=" + value.words.length);
    let ret = alasql('select count(*) as cnt from ? where v=?', [value.words, 'UDOH']);
    console.log("count=" + ret[0].cnt);
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
});
*/