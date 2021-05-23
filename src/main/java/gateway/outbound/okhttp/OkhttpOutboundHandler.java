package gateway.outbound.okhttp;

import com.google.common.base.Throwables;
import gateway.filter.HeaderHttpResponseFilter;
import gateway.filter.HttpRequestFilter;
import gateway.filter.HttpResponseFilter;
import gateway.outbound.httpclient4.NamedThreadFactory;
import gateway.router.HttpEndpointRouter;
import gateway.router.WeightRandomHttpEndpointRouter;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import okhttp3.*;
import org.apache.http.protocol.HTTP;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class OkhttpOutboundHandler {

    /**
     * 后端服务url列表
     */
    private final List<String> backendsAndWeight;


    private final ExecutorService proxyService;

    HttpResponseFilter filter = new HeaderHttpResponseFilter();
    HttpEndpointRouter router = new WeightRandomHttpEndpointRouter();

    /**
     * 初始话http处理器
     *
     * @param backendsAndWeight 后端接口及权重
     */
    public OkhttpOutboundHandler(List<String> backendsAndWeight) {

        this.backendsAndWeight = backendsAndWeight;
        int cores = Runtime.getRuntime().availableProcessors();
        long keepAliveTime = 1000;
        int queueSize = 2048;
        // 定义线程池丢弃策略
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();//.DiscardPolicy();
        // 创建线程池，由于IO密集型，所以核心线程数和最大线程数设置的比较大
        proxyService = new ThreadPoolExecutor(2 * (cores + 1), 4 * (cores + 1),
                keepAliveTime, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueSize),
                new NamedThreadFactory("okHttpHandler"), handler);
    }

    /**
     * 去除域名尾巴的后的/
     *
     * @param backend 后端接口地址
     * @return 规范后的域名
     */
    private String formatUrl(String backend) {
        return backend.endsWith("/")?backend.substring(0,backend.length()-1):backend;
    }

    /**
     * 处理请求
     */
    public void handle(final FullHttpRequest fullRequest, final ChannelHandlerContext ctx, HttpRequestFilter filter) {
        // 使用加权随机的路由方式
        String backendUrl = router.route(this.backendsAndWeight);
        // 拼接真正请求数据的url
        final String url = formatUrl(backendUrl) + fullRequest.uri();
        // 对请求进行过滤
        filter.filter(fullRequest, ctx);
        // 提交线程池进行后端请求
        proxyService.submit(()->async(fullRequest, ctx, url));
    }

    /**
     * 整合okhttp请求
     */
    private void async(final FullHttpRequest inbound, final ChannelHandlerContext ctx, final String url) {

        // 拼接自定义的header
        Headers.Builder headersbuilder = new Headers.Builder();
        headersbuilder.add(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        headersbuilder.add("X-FROM", "NettyGateway");
        Headers headers = headersbuilder.build();

        // 请求拼接好的url
        OkHttpClient okHttpClient = new OkHttpClient();
        final Request request = new Request.Builder()
                .url(url)
                .get()//默认就是GET请求，可以不写
                .headers(headers)
                .build();
        Call call = okHttpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                System.out.println("has fail. e :" + Throwables.getStackTraceAsString(e));
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                try {
                    // 成功获取到返回后处理返回值
                    handleResponse(inbound, ctx, response);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void handleResponse(final FullHttpRequest fullRequest, final ChannelHandlerContext ctx, final Response endpointResponse) throws Exception {
        FullHttpResponse response = null;
        try {
            // 获取从后端服务获取到的body
            byte[] body = endpointResponse.body().bytes();
            // 拼接自定义的header
            response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(body));
            response.headers().set("Content-Type", "application/json");
            response.headers().setInt("Content-Length", Integer.parseInt(endpointResponse.header("Content-Length")));

            // 对返回内容进行过滤
            filter.filter(response);

        } catch (Exception e) {
            e.printStackTrace();
            response = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT);
            exceptionCaught(ctx, e);
        } finally {
            // 请求对象不为空则将返回内容写回给客户端
            if (fullRequest != null) {
                if (!HttpUtil.isKeepAlive(fullRequest)) {
                    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    ctx.write(response);
                }
            }
            ctx.flush();
        }

    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
