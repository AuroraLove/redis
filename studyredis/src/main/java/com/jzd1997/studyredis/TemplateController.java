package com.jzd1997.studyredis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.alibaba.fastjson.JSON;
import com.jzd1997.studyredis.util.RedisUtils;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Controller
public class TemplateController {
	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	RedisUtils utils;
	
    @ApiOperation(value = "操作字符串/整数/浮点数", notes = "字符串", httpMethod = "GET")
    @GetMapping(value = "/setString")
    @ResponseBody
    public String setString(@ApiParam(value = "存入key", required = true) @RequestParam String key,
    		 @ApiParam(value = "存入value", required = true) @RequestParam String value) {
    	utils.setValue(key,value,100,TimeUnit.SECONDS);
    	String ret = utils.getValue(key).toString();
    	return ret;
    }

    @ApiOperation(value = "一次性设置多组数据，字符串/整数/浮点数", notes = "多组数据", httpMethod = "POST")
    @PostMapping(value = "/multiSet")
    @ResponseBody
    public List<Object> multiSet(@ApiParam(value = "存入key/value", required = true) @RequestBody Map keys) {
    	utils.multiSet(keys);
    	List<Object> ret = utils.multiGet(keys.keySet());
    	return ret;
    }

    @ApiOperation(value = "取号器 增量", notes = "取号器", httpMethod = "GET")
    @GetMapping(value = "/incr")
    @ResponseBody
    public long incr(@ApiParam(value = "key", required = true) @RequestParam String key,
    						@ApiParam(value = "增量 整数", required = true) @RequestParam long delta) {
    	return utils.incr(key, delta);
    }

    @ApiOperation(value = "插入队列", notes = "插入队列", httpMethod = "GET")
    @GetMapping(value = "/lpush")
    @ResponseBody
    public List<Object> lpush(@ApiParam(value = "key", required = true) @RequestParam String key,
    						@ApiParam(value = "内容", required = true) @RequestParam String content) {
    	utils.lpush(key, content);
    	return utils.range(key, 0, -1);
    }

    @ApiOperation(value = "推出队列", notes = "推出队列", httpMethod = "GET")
    @GetMapping(value = "/rpop")
    @ResponseBody
    public Object rpop(@ApiParam(value = "key", required = true) @RequestParam String key) {
    	return utils.rpop(key);
    }

    @ApiOperation(value = "Hashset", notes = "Hashset", httpMethod = "GET")
    @GetMapping(value = "/setHash")
    @ResponseBody
    public void setHash(@ApiParam(value = "key", required = true) @RequestParam String key,
    		@ApiParam(value = "Json字符串", required = true) @RequestParam String json) {
    	Map map = (Map)JSON.parse(json);
    	utils.setHash(key, map);
    }

    @ApiOperation(value = "HashGetAll", notes = "HashGetAll", httpMethod = "GET")
    @GetMapping(value = "/getHashAll")
    @ResponseBody
    public Map getHashAll(@ApiParam(value = "key", required = true) @RequestParam String key) {
    	return utils.getHashAll(key);
    }

    @ApiOperation(value = "HashGet", notes = "HashGet", httpMethod = "GET")
    @GetMapping(value = "/getHash")
    @ResponseBody
    public Object getHash(@ApiParam(value = "key", required = true) @RequestParam String key,
    					@ApiParam(value = "prop", required = true) @RequestParam String prop) {
    	
    	Object ret = utils.getHash(key, prop);
    	log.info("key:" + key + " prop:" + prop + " val:" + ret.toString());
    	return ret;
    }

    @ApiOperation(value = "订阅", notes = "subscribe", httpMethod = "GET")
    @GetMapping(value = "/subscribe")
    @ResponseBody
    public void subscribe(@ApiParam(value = "渠道", required = true) @RequestParam String channel) throws InterruptedException {   	
    	utils.subscribe(channel);
    }

    @ApiOperation(value = "发布", notes = "publish", httpMethod = "GET")
    @GetMapping(value = "/publish")
    @ResponseBody
    public void publish(@ApiParam(value = "渠道", required = true) @RequestParam String channel,
    		@ApiParam(value = "消息", required = true) @RequestParam String message) throws InterruptedException {   	
    	utils.publish(channel, message);
    }
}
