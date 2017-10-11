var xmldom = require('xmldom');
var DOMParser = xmldom.DOMParser;
var XMLSerializer = xmldom.XMLSerializer
var xpath = require('xpath');
var SimpleMSSQL = require('simple-mssql');
var alasql = require('alasql');
var crypto = require('crypto');
var _ = require('lodash');
var Promise = require('promise');
var fs = require('fs');
var uuid = require('node-uuid');
var os = require('os');
var path = require('path');
var AWSS3Https = require('aws-s3-https');

	function object2Element(doc, objName, obj) {
		var el = doc.createElement(objName);
		for (var fld in obj) {	// for each field
			var v = obj[fld];
			if (typeof v === 'boolean')
				el.setAttribute(fld, v ? '1' : '0');
			else if (typeof v === 'number')
				el.setAttribute(fld, v.toString());
			else if (v) 
				el.setAttribute(fld, v.toString());
		}
		return el;
	}
	
	function documentInfos2XML(documentInfos) {
		var doc = new DOMParser().parseFromString('<?xml version="1.0"?>', 'text/xml');
		var root = doc.createElement('result');
		doc.appendChild(root);
		var elDocs = doc.createElement('docs');
		root.appendChild(elDocs);
		var elPages = doc.createElement('pages');
		root.appendChild(elPages);
		var elLines = doc.createElement('lines');
		root.appendChild(elLines);
		var elWords = doc.createElement('words');
		root.appendChild(elWords);
		for (var docId in documentInfos) {	// for each document
			var documentInfo = documentInfos[docId];
			elDocs.appendChild(object2Element(doc, 'd', _.assignIn({sp: docId}, documentInfo.doc)));
			for (var i in documentInfo.pages) {	// for each page
				var pg = _.assignIn({sp: docId}, documentInfo.pages[i]);
				elPages.appendChild(object2Element(doc, 'p', pg));
			}
			for (var i in documentInfo.lines) {	// for each line
				var ln = _.assignIn({sp: docId}, documentInfo.lines[i]);
				elLines.appendChild(object2Element(doc, 'l', ln));
			}
			for (var i in documentInfo.words) {	// for each word
				var wd = _.assignIn({sp: docId}, documentInfo.words[i]);
				elWords.appendChild(object2Element(doc, 'w', wd));
			}
		}
		var xml = new XMLSerializer().serializeToString(doc);
		return xml;
	}
	
	function getBoundFromNode(node) {
		var top = parseInt(node.getAttribute('t'));
		var bottom = parseInt(node.getAttribute('b'));
		var left = parseInt(node.getAttribute('l'));
		var right = parseInt(node.getAttribute('r'));
		return {t: top, b: bottom, l: left, r: right};
	}

	function processPage(page, doc) {
		var select = xpath.useNamespaces({"a": "http://www.scansoft.com/omnipage/xml/ssdoc-schema3.xsd"});
		var nl = select('//a:ln', doc);
		
		var midPoints = {};
		var lineNodeInfos = {};
		for (var i = 0; i < nl.length; i++) {	// for each line
			var lineNode = nl[i];
			var bound = getBoundFromNode(lineNode);
			var midPoint = Math.round(parseFloat(bound.t + bound.b)/2.0);
			midPoints[midPoint] = true;
			var key = JSON.stringify(bound);
			lineNodeInfos[key] = {node: nl[i], bound: bound};
		}
		var count = 0;
		var mids = [];
		for (var midPoint in midPoints)
			mids.push(parseInt(midPoint));
		//console.log(mids.length);
		mids.sort(function(a, b) {return a-b;});
		//console.log(mids);
		var pageLines = [];
		var pageWords = [];
		for (var i in mids) {	// for each logical line
			var line_no = parseInt(i);
			var midPoint = mids[i];
			//console.log('I am here 1, mid = ' + midPoint);
			var tempLines = [];
			for (var key in lineNodeInfos) {
				var lineInfo = lineNodeInfos[key];
				var bound = lineInfo.bound;
				if ((bound.t - 5) <= midPoint && midPoint <= (bound.b + 5)) {
					tempLines.push(lineInfo);
				}
			}
			// sort the lines by their left position in ascending order
			tempLines.sort(function(a, b) {
				return (a.bound.l - a.bound.l);
			});
			
			//console.log('I am here 2: ' + tempLines.length);
			var lineWords = [];	// words for the logical line
			var wordsInLogicalLine = [];
			for (var j in tempLines) {	// for each line in the logical line group
				var lineInfo = tempLines[j];
				var lineNode = lineInfo.node;
				var wordNodes = select('a:wd', lineNode);
				var words = [];
				var words_ = [];
				for (var k = 0; k < wordNodes.length; k++) {	// for each word in the line
					var wordNode = wordNodes[k];
					var bound = getBoundFromNode(wordNode);
					var word = wordNode.textContent.trim().replace(/\s+/g, "");
					words.push(word);
					words_.push({pg: page, ln: line_no, v: word, l: bound.l, r: bound.r, t: bound.t, b: bound.b});
				}
				wordsInLogicalLine = wordsInLogicalLine.concat(words);
				lineWords = lineWords.concat(words_);
				var key = JSON.stringify(lineInfo.bound);
				delete lineNodeInfos[key];
			}
			for (var j in lineWords)	// for each word
				lineWords[j].idx = parseInt(j);
				
			pageWords = pageWords.concat(lineWords);
			var line = {pg: page, ln: line_no, v: wordsInLogicalLine.join(' '), wds: wordsInLogicalLine.length};
			var lineBounds = alasql('SELECT min(l) as l, max(r) as r, min(t) as t, max(b) as b from ?',[lineWords])[0];
			//console.log(JSON.stringify(lineBounds));
			var noBound = (JSON.stringify(lineBounds) === '{}');
			line.nb = noBound;
			line.t = (noBound ? null : lineBounds.t);
			line.b = (noBound ? null : lineBounds.b);
			line.l = (noBound ? null : lineBounds.l);
			line.r = (noBound ? null : lineBounds.r);
			pageLines.push(line);
		}
		
		//console.log(JSON.stringify(pageLines));
		var ret = alasql('SELECT min(l) as l, max(r) as r, min(t) as t, max(b) as b from ? where nb=false',[pageLines]);
		var pi = ret[0];
		pi = _.assignIn({pg: page, lns: pageLines.length}, pi);
		//console.log('page ' + page + ': ' + JSON.stringify(pi));
		return {lines: pageLines, words: pageWords, pi: pi};
	}

	function aggregatePageInfos(pageInfos) {
		pageInfos.sort(function(a, b) {return a.page-b.page;});	// sort by page number asc
		var documentInfo = {
			pages: []
			,lines: []
			,words: []
		}
		var topOffset = 0;
		var lineOffset = 0;
		for (var i in pageInfos) {	// for each page
			//console.log([topOffset, lineOffset]);
			var page = pageInfos[i].page;
			var pageInfo = pageInfos[i].pageInfo;
			var pi = pageInfo.pi;
			var shiftedPage = _.assignIn({}, pi, {at: pi.t + topOffset, ab: pi.b + topOffset});
			documentInfo.pages.push(shiftedPage);
			var shiftedWords = alasql('SELECT pg, ln, idx, v, l, r, t, b, ln+' + lineOffset + ' as aln, if(t, t+' + topOffset + ', null) as at, if(b, b+' + topOffset + ', null) as ab from ? ', [pageInfo.words]);
			documentInfo.words = documentInfo.words.concat(shiftedWords);
			var shiftedLines = alasql('SELECT pg, ln, v, wds, l, r, t, b, ln+' + lineOffset + ' as aln, if(t, t+' + topOffset + ', null) as at, if(b, b+' + topOffset + ', null) as ab, nb from ? ', [pageInfo.lines]);
			documentInfo.lines = documentInfo.lines.concat(shiftedLines);
			//console.log('page ' + page + ': ' + JSON.stringify(newLines));
			topOffset += (pageInfo.pi.b + 100);
			lineOffset += pageInfo.lines.length;
		}
		var ret = alasql('select sum(lns) as lns, min(l) as l, max(r) as r, min(at) as at, max(ab) as ab from ?',[documentInfo.pages]);
		documentInfo.doc = _.assignIn({pgs: documentInfo.pages.length}, ret[0]);
		return documentInfo;
	}
	
	function getDocumentInfosFromS3XMLs(docInfos, done) {
		var totalPagesToDownload = 0;
		var numPagesLeftByDoc = {};	// by docId
		for (var i in docInfos) {
			var docInfo = docInfos[i];
			var docId = docInfo.start_page;
			if (typeof numPagesLeftByDoc[docId] != 'number') numPagesLeftByDoc[docId] = 0;
			numPagesLeftByDoc[docId] += docInfo.num_page;
			totalPagesToDownload += docInfo.num_page;
		}
		console.log('number doc instance: ' + docInfos.length + ', total number pages to retrieve from S3: ' + totalPagesToDownload);
		var outstanding = totalPagesToDownload;
		var errors = [];
		var pageInfos = {};	// by docId
		var documentInfos = {};	// by docId
		var downloadInfos = [];
		
		function getXMLCompleteHandler(i) {
			return (function(err, xml) {
				var di = downloadInfos[i];
				var docId = di.docId;
				var page = di.page;
				var r_page = page - parseInt(docId) + 1;	// relative page number to the document
				var pageIdentifier = JSON.stringify({docId: docId, page: page});
			
				outstanding--;
				numPagesLeftByDoc[docId]--;
				if (err) {
					errors.push({docId: docId, page: page, err: err});
				} else {
					//console.log(xml);
					console.log('parsing ' + pageIdentifier + ' using dom...');
					var doc = new DOMParser().parseFromString(xml.length == 0 ? '<?xml version="1.0" encoding="UTF-16"?>' : xml, 'text/xml');
					console.log('successfully parsing ' + pageIdentifier + ' using dom :-)');
					var pageInfo = processPage(r_page, doc);
					if (!pageInfos[docId]) pageInfos[docId] = [];
					pageInfos[docId].push({page: r_page, pageInfo: pageInfo});
				}
				if (numPagesLeftByDoc[docId] === 0) {	// this document done
					var documentInfo = aggregatePageInfos(pageInfos[docId]);
					documentInfos[docId] = documentInfo;
				}
				if (outstanding === 0) {	// all done
					console.log('all pages downloaded from S3');
					if (errors.length === 0) {
						//setTimeout(function() {done(null, documentInfos);}, 10000);
						done(null, documentInfos);
					} else {
						errors.sort(function(a, b) {return a.page-b.page;});
						done(errors, null);
					}
				} else {
					console.log('S3 download progress: ' + (totalPagesToDownload-outstanding) + '/' + totalPagesToDownload);
					//GetS3XmlPageContent(downloadInfos[i+1].deal_name, downloadInfos[i+1].loan_id, downloadInfos[i+1].page, downloadInfos[i+1].bucket, getXMLCompleteHandler(i+1));
				}
			});
		}
