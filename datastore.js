var exports = module.exports={};
var MongoClient = require('mongodb').MongoClient;
var dataCollect = require('./datacollect.js');
var url ='mongodb://localhost:27017/freedolar';
var timeout;
var dbo;
var io;

MongoClient.connect(url, { useNewUrlParser: true } ,function(err, db){
    if (err) throw err;
    dbo = db.db('freedolar');
});


exports.saveQuote = function(time, bid, ask, mid, date, currency){
    appTimeOut(time);
    var newDate = this.cleanDate(date);
    console.log("Arrived BID: "+bid);
    console.log("Arrived ASK: "+ask);
    var checkSpr = checkSpread(bid,ask);
    /*MongoClient.connect(url, { useNewUrlParser: true } ,function(err, db){
        if (err) throw err;
        var dbo = db.db('freedolar');*/
        if(bid != null && ask != null && mid != null){
            if(isNaN(bid) || isNaN(ask) || isNaN(mid)){
                console.log("Object not inserted, one is NaN: "+bid+" "+ask+" "+mid);
            }
            else if(checkSpr == false){
                console.log("Sorry, cannot insert a value with high bid-offer spread");
            }
            else{
                var collectionTick = currency + '.tick'
                dataCollect.getOHLC(newDate,1,collectionTick,collectionTick,insertTick);//tick insertion
                console.log("Inserting in collection: "+collectionTick);
                /*setTimeout(function (){
                    dataCollect.getHistory(10,collectionTick,function (data){
                    streamer.sendLive(io, data);
                    });
                }
                ,1000);*/

                if(newDate.getSeconds() == '00'){//minute by minute insertion
                    var collection = currency + '.minute';
                    dataCollect.getOHLC(newDate,1,collectionTick,collection,insertTick);
                }

                if(Number.isInteger(newDate.getMinutes()/5) && (newDate.getSeconds() == '00')){//5 minutes insertion
                    var collection = currency + '.fiveminutes';
                    dataCollect.getOHLC(newDate,5,collectionTick,collection,insertTick);
                }

                if(Number.isInteger(newDate.getMinutes()/10) && (newDate.getSeconds() == '00')){//10 minutes insertion
                    var collection = currency + '.tenminutes';
                    dataCollect.getOHLC(newDate,10,collectionTick,collection,insertTick);
                    /*setTimeout(function (){
                        dataCollect.getHistory(1440,collection,function (data){
                        streamer.sendHistory(io, data);
                        });
                    }
                    ,1000);*/

                }

                if(Number.isInteger(newDate.getMinutes()/30) && (newDate.getSeconds() == '00')){//30 minutes insertion
                    var collection = currency + '.thirtyminutes';
                    dataCollect.getOHLC(newDate,30,collectionTick,collection,insertTick);
                }

                if((newDate.getMinutes() == '00') && (newDate.getSeconds() == '00')){//1 hour insertion
                    var collection = currency + '.onehour';
                    dataCollect.getOHLC(newDate,60,collectionTick,collection,insertTick);
                }

                if(Number.isInteger(newDate.getHours()/6) && (newDate.getMinutes() == '00') && (newDate.getSeconds() == '00')){//6 hours insertion
                    var collection = currency + '.sixhours';
                    dataCollect.getOHLC(newDate,360,collectionTick,collection,insertTick);
                }

                if(Number.isInteger(newDate.getHours()/12) && (newDate.getMinutes() == '00') && (newDate.getSeconds() == '00')){//12 hours insertion
                    var collection = currency + '.twelvehours';
                    dataCollect.getOHLC(newDate,720,collectionTick,collection,insertTick);
                }

                if((newDate.getHours() == '00') && (newDate.getMinutes() == '00') && (newDate.getSeconds() == '00')){//Daily insertion
                    var collection = currency + '.daily';
                    dataCollect.getOHLC(newDate,1440,collectionTick,collection,insertDaily);
                }
            }
        }
        function insertTick(ohlc, collection){
            if (!Array.isArray(ohlc) || !ohlc.length){
                var data = {bid:bid, ask:ask, mid:mid, date:newDate, high:mid, low:mid, open:mid, close:mid};
                dbo.collection(collection).insertOne(data, function(err,res){//tick by tick insertion by default
                    if (err) throw err;
                    //setTimeout(function(){db.close()},2000);

                });  
            }
            else{
                var data = {bid:bid, ask:ask, mid:mid, date:newDate, high:ohlc[0].high, low:ohlc[0].low, open:ohlc[0].open, close:ohlc[0].close};
                dbo.collection(collection).insertOne(data, function(err,res){//tick by tick insertion by default
                    if (err) throw err;
                    //setTimeout(function(){db.close()},2000);
                });  
            }
            
        }

        function insertDaily(ohlc, collection){
                var date = new Date(newDate);
                var dateInsert = new Date(date.setDate(date.getDate()-1));

                if (!Array.isArray(ohlc) || !ohlc.length){
                    
                    var data = {bid:bid, ask:ask, mid:mid, date:dateInsert, high:mid, low:mid, open:mid, close:mid};
                    dbo.collection(collection).insertOne(data, function(err,res){//tick by tick insertion by default
                        if (err) throw err;
                        //setTimeout(function(){db.close()},2000);
    
                    });  
                }
                else{
                    var data = {bid:bid, ask:ask, mid:mid, date:dateInsert, high:ohlc[0].high, low:ohlc[0].low, open:ohlc[0].open, close:ohlc[0].close};
                    dbo.collection(collection).insertOne(data, function(err,res){//tick by tick insertion by default
                        if (err) throw err;
                        //setTimeout(function(){db.close()},2000);
                    });  
                }
            
        }

}

exports.cleanDate=function (passDate){
    var date = new Date(passDate);
    var seconds = date.getSeconds()/timeout;
    seconds = Math.round(seconds)*timeout;
    date.setMilliseconds(0);
    date.setSeconds(seconds);
    return date;
}

function appTimeOut(time){
    timeout = time/1000;
}

exports.setIO=function(ion){
 io = ion;
}

function checkSpread(bid,ask){
    var spread = (parseFloat(bid)-parseFloat(ask));
    var spreadAbs = Math.abs(spread);
    var sp1 = spreadAbs/(parseFloat(bid)+parseFloat(ask));
    if (sp1 < 0.3){
        return true;
    }
    else {
        return false;
    }
}