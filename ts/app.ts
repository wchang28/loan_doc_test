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

interface Bound {
    t?: number;  // top
    b?: number;  // bottom
    l?: number;  // left
    r?: number;  // right
}

interface LineNodeInfo {
    node: Element;
    bound: Bound;
}

interface WordInfo extends Bound {
    pg?: number; // page number
    ln?: number; // line number
    v?: string;  // value
    idx?: number;   // word index in the line
}

interface LineInfo extends Bound {
    pg?: number; // page number
    nb?: boolean
    ln?: number; // line number
    v?: string; // value
    wds?: number;    // number of words
}

interface PageInfo extends Bound {
    pg?: number; // page number
    lns?: number; // number of line in the page
}

interface PageExtraction {
    pi: PageInfo
    lines: LineInfo[]
    words: WordInfo[];
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
    let pi: PageInfo = _.assignIn({pg: page, lns: pageLines.length}, ret[0]);
    //console.log('page ' + page + ': ' + JSON.stringify(pi));
    return {pi, lines: pageLines, words: pageWords};
}

function getS3XMLDoc(Bucket: string, Key: string) : Promise<Document> {
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

function getPageExtractionFromS3(Bucket: string, Key: string, page: number) : Promise<PageExtraction> {
    return getS3XMLDoc(Bucket, Key).then((doc: Document) => processPage(page, doc));
}

function pad_4_zeros(page: number) : string {
    if (page < 10)
        return "000" + page.toString();
    else if (page < 100)
        return "00" + page.toString();
    else if (page < 1000)
        return "0" + page.toString();
    else
        return page.toString();
}

let Bucket = "harvest-split";
/*
getPageExtractionFromS3(Bucket, "162de65ed655c5a7328b535c7a716994/fdd1f16370c53f777a83df5320b6a899/TXT/page_0001.xml", 1)
.then((pe: PageExtraction) => {
    console.log(JSON.stringify(pe, null, 2));
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
})
*/

let pages = 21;
let promises: Promise<PageExtraction>[] = [];
for (let i = 0; i < pages; i++) {
    let page = i + 1;
    let Key = "162de65ed655c5a7328b535c7a716994/fdd1f16370c53f777a83df5320b6a899/TXT" + "/" + "page_" + pad_4_zeros(page) + ".xml";
    promises.push(getPageExtractionFromS3(Bucket, Key, page));
}
let p = Promise.all(promises);
p.then((value: PageExtraction[]) => {
    console.log(value.length);
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
});