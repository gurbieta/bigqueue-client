package bigqueue;

import java.util.Arrays;

import javax.ws.rs.core.MediaType;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.client.apache.ApacheHttpClient;
import com.sun.jersey.client.apache.ApacheHttpClientHandler;
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig;

/**
 * @author gurbieta
 * @date Feb 4, 2013
 */
public class BigQueueClient {

	private static final int	MAX_CONNECTIONS_PER_HOST	= 20;
	private static final int	MAX_TOTAL_CONNECTIONS		= 20;

	private final WebResource	resource;

	private final String		topic;
	private final String		consumer;

	public BigQueueClient(String url, String topic, String consumer) {
		this.topic = topic;
		this.consumer = consumer;

		DefaultApacheHttpClientConfig clientConfig = new DefaultApacheHttpClientConfig();
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		connectionManager.getParams().setDefaultMaxConnectionsPerHost(MAX_CONNECTIONS_PER_HOST);
		connectionManager.getParams().setMaxTotalConnections(MAX_TOTAL_CONNECTIONS);

		Client c = new ApacheHttpClient(new ApacheHttpClientHandler(new HttpClient(connectionManager)), clientConfig);
		c.setConnectTimeout(3000);
		c.setReadTimeout(3000);

		this.resource = c.resource(url);
	}

	public JSONObject get() {
		return resource.path("topics").path(topic).path("consumers").path(consumer).path("messages").get(JSONObject.class);
	}

	public boolean ack(String recipientCallback) {
		try {
			resource.path("topics").path(topic).path("consumers").path(consumer).path("messages").path(recipientCallback)
					.delete();
		} catch (Exception e) {
			LoggerFactory.getLogger(BigQueueClient.class).error("Can't ack [" + recipientCallback + "]", e);
			return false;
		}
		return true;
	}

	public boolean put(JSONObject message) {
		JSONObject msg = new JSONObject();
		try {
			msg.put("msg", message);
			msg.put("topics", Arrays.asList(topic));
		} catch (JSONException e) {
			LoggerFactory.getLogger(BigQueueClient.class).error("Can't create message [" + message + "]", e);
			return false;
		}

		try {
			resource.path("messages").type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
					.post(String.class, msg.toString());
		} catch (Exception e) {
			LoggerFactory.getLogger(BigQueueClient.class).error("Can't post message [" + msg + "]", e);
			return false;
		}
		return true;
	}

}
