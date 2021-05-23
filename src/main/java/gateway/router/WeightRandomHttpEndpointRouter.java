package gateway.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WeightRandomHttpEndpointRouter implements HttpEndpointRouter {

    @Override
    public String route(List<String> urlAndWeight) {
        List<String> urls = new ArrayList<>();
        List<Integer> weights = new ArrayList<>();
        int sum = 0;
        // 解析参数拿到全部的后端服务url 和 权重配置
        for (String item : urlAndWeight) {
            String[] params = item.split("->");
            urls.add(params[0]);
            int weight = Integer.parseInt(params[1]);
            weights.add(weight);
            sum = sum + weight;
        }
        // 根据权重的总和随机一个随机数
        int randomValue = new Random().nextInt(sum);
        int temp = 0;
        int index = 0;
        // 随机数落在的区间就是随机到的后端服务url
        for (Integer weight : weights) {
            temp = temp + weight;
            if (randomValue < temp) {
                break;
            }
            index ++;
        }
        // 返回加权随机到的后端服务。
        return urls.get(index);
    }
}
