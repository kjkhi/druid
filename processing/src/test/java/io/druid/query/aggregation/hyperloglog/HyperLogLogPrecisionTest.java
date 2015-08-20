package io.druid.query.aggregation.hyperloglog;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.query.search.SearchQueryTest;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by jaykelin on 2015/7/23.
 */
public class HyperLogLogPrecisionTest {

    private final HashFunction fn = Hashing.murmur3_128();

    @Test
    public void tt(){
//        Random random = new Random();
//        for(int i=0; i<10;i++){
//            System.out.println(random.nextInt(10)+1);
//        }

        String s = "11{2sssss1}";
        System.out.println(s.substring(s.indexOf('{'), s.lastIndexOf('}')));
    }

    @Test
    public void precisionTest(){
        long l = System.currentTimeMillis();
        int count = 3000000;
        int totalCount = 0;
        double d = 0;
        double maxd = 0;
        double mind = Double.MAX_VALUE;
        Random random = new Random();
        List<Double> list = new ArrayList<>();
        HyperLogLogCollector fh = HyperLogLogCollector.makeLatestCollector();
//        Set<Integer> totalSet = new HashSet<>();
        for(int j =0; j<100; j++){
            Set<Integer> set = new HashSet<>();
            HyperLogLogCollector hll = HyperLogLogCollector.makeLatestCollector();
            for(int i =0; i< count; i++){
                int item = nextInt(0, Integer.MAX_VALUE);
//                int item = random.nextInt();
                set.add(item);
                hll.add(fn.hashInt(item).asBytes());
            }
            fh.fold(hll);
            double dd= (hll.estimateCardinality() - set.size())/set.size()*100;
            double ddd = Math.abs(dd);
            list.add(ddd);
            d += ddd;
            if(maxd < ddd){
                maxd = ddd;
            }
            if(mind > ddd){
                mind = ddd;
            }
//            System.out.println("估算值："+hll.estimateCardinality());
//            System.out.println("真实值："+set.size());
            System.out.println("精度："+dd);
//            totalSet.addAll(set);
            set = null;
        }
        double avg = d/100d;
        double t=0;
        for(double i : list){
            t += Math.pow(i-avg, 2);
        }
        System.out.println("------------- 基数在"+count+"左右，对100次求合并与平均 ---------------");
        System.out.println("合并后的估算值："+fh.estimateCardinality());
//        System.out.println("合并后的估算误差："+(fh.estimateCardinality() - totalSet.size())/totalSet.size()*100+"%");
        System.out.println("100次平均误差："+avg+"%");
        System.out.println("标准差："+Math.sqrt(t/100));
        System.out.println("最大误差："+maxd+"%");
        System.out.println("最小误差：" + mind + "%");
        System.out.println("耗时："+(System.currentTimeMillis()-l)/1000+"秒");
    }

    public int nextInt(final int min, final int max){
        Random rand= new Random();
        int tmp = Math.abs(rand.nextInt());
        return tmp % (max - min + 1) + min;

    }
}
