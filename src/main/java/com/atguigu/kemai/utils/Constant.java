package com.atguigu.kemai.utils;

/**
 * Created by JarvisKwok
 * Date :2020/10/28
 * Description :
 */
public class Constant {
    /**
     * 机构类型
     */
    // 1、港澳台
    public static StringBuilder gangaotai = new StringBuilder();

    static {
        gangaotai.append("台、港、澳投资企业分公司");
        gangaotai.append("#台港澳法人独资");
        gangaotai.append("#台港澳合资");
        gangaotai.append("#台港澳与境内合资");
        gangaotai.append("#台港澳自然人独资");
        gangaotai.append("#港、澳、台其他");
    }

    //2、外资
    public static StringBuilder waizi = new StringBuilder();

    static {
        waizi.append("外商投资企业分公司");
        waizi.append("#外国法人独资");
        waizi.append("#外商合资");
        waizi.append("#外商投资企业法人独资");
        waizi.append("#中外合资");
        waizi.append("#外商投资企业法人独资");
        waizi.append("#外资其他");
    }

    //3、国营
    public static StringBuilder guoying = new StringBuilder();

    static {
        guoying.append("国有控股");
        guoying.append("#上市、国有控股");
        guoying.append("#国有经济");
        guoying.append("#非法人");
        guoying.append("#国有事业单位营业");
        guoying.append("#非法人");
        guoying.append("#集体经济");
        guoying.append("#非法人");
        guoying.append("#集体事业单位营业");
        guoying.append("#集体所有制");
        guoying.append("#股份合作");
        guoying.append("#全民所有制");
        guoying.append("#非法人");
        guoying.append("#国有独资");
        guoying.append("#国有控股");
        guoying.append("#国有独资");
        guoying.append("#国有控股");
        guoying.append("#国有其他");
    }

    //4、民营
    public static StringBuilder minying = new StringBuilder();

    static {
        minying.append("非公司私营企业");
        minying.append("#分公司");
        minying.append("#个人独资企业");
        minying.append("#个人独资企业分支机构");
        minying.append("#股份合作制");
        minying.append("#股份合作制分支机构");
        minying.append("#非上市");
        minying.append("#非上市、自然人投资或控股");
        minying.append("#股份有限公司分公司");
        minying.append("#非上市");
        minying.append("#非上市、自然人投资或控股");
        minying.append("#上市");
        minying.append("#非法人");
        minying.append("#联营");
        minying.append("#内资非法人企业、非公司私营企业及内资非公司企业分支机构");
        minying.append("#内资分公司");
        minying.append("#普通合伙企业");
        minying.append("#非上市");
        minying.append("#非上市");
        minying.append("#上市");
        minying.append("#其他有限责任公司");
        minying.append("#其他有限责任公司分公司");
        minying.append("#自然人控股或私营性质企业控股");
        minying.append("#内资法人独资");
        minying.append("#私营法人独资");
        minying.append("#自然人独资");
        minying.append("#一人有限责任公司分公司");
        minying.append("#有限合伙");
        minying.append("#有限合伙企业");
        minying.append("#有限责任公司");
        minying.append("#法人独资");
        minying.append("#非自然人投资或控股的法人独资");
        minying.append("#自然人独资");
        minying.append("#自然人投资或控股");
        minying.append("#自然人投资或控股的法人独资");
        minying.append("#有限责任公司分公司");
        minying.append("#法人独资");
        minying.append("#非自然人投资或控股的法人独资");
        minying.append("#自然人独资");
        minying.append("#自然人投资或控股");
        minying.append("#自然人投资或控股的法人独资");
        minying.append("#民营其他");
    }

    //5、农资
    public static StringBuilder nongzi = new StringBuilder();

    static {
        nongzi.append("农民专业合作经济组织");
        nongzi.append("#农民专业合作社");
        nongzi.append("#农民专业合作社分支机构");
        nongzi.append("#农资其他");
    }

    //6、个体户
    public static StringBuilder geti = new StringBuilder();

    static {
        geti.append("个体");
        geti.append("#个体工商户");
        geti.append("#个体户");
        geti.append("#个体户其他");
    }

    //7、社会团体
    public static StringBuilder shehui = new StringBuilder();

    static {
        shehui.append("非政府组织");
        shehui.append("#非营利组织");
        shehui.append("#公民社会");
        shehui.append("#第三部门或独立部门");
        shehui.append("#志愿者组织");
        shehui.append("#慈善组织");
        shehui.append("#免税组织");
    }

    /**
     * contactsource分类：
     * 年报
     * 官网
     * B2B
     * 招聘
     * 垂直门户
     * 分类信息
     * 地图
     * 企业信息
     * 生活服务
     * O2O平台
     * 招标信息
     * 资讯
     * 资料平台
     * 智能挖掘
     */

    public static StringBuilder B2B = new StringBuilder();

    static {
        B2B.append("北极星电力商务通");
        B2B.append("#推广家");
        B2B.append("#100招商网");
        B2B.append("#企业第一网");
        B2B.append("#中科商务网");
        B2B.append("#凡搜网");
        B2B.append("#1024商务网");
        B2B.append("#全商网");
        B2B.append("#自助贸易中国站");
        B2B.append("#无忧商务网");
        B2B.append("#环球经贸网");
        B2B.append("#东商网");
        B2B.append("#互联网服务平台");
        B2B.append("#万国企业网");
        B2B.append("#拓客多资源");
        B2B.append("#八方资源网");
        B2B.append("#中贸网");
        B2B.append("#恒商网");
        B2B.append("#商路通");
        B2B.append("#慧聪网");
        B2B.append("#阿土伯");
        B2B.append("#勤发网");
        B2B.append("#环球商贸网");
        B2B.append("#企业发布网");
        B2B.append("#爱喇叭网");
        B2B.append("#商国互联");
        B2B.append("#商牛网");
        B2B.append("#买购网");
        B2B.append("#企库网");
        B2B.append("#万商汇");
        B2B.append("#首商网");
        B2B.append("#十环网");
        B2B.append("#搜了网");
        B2B.append("#国际企业网");
        B2B.append("#国联资源网");
        B2B.append("#万国商务网");
        B2B.append("#众加商贸网");
        B2B.append("#马可波罗网");
        B2B.append("#厂家贸易网");
        B2B.append("#一大把");
        B2B.append("#易登网");
        B2B.append("#搜好资源网");
        B2B.append("#企页网");
        B2B.append("#一比多企业库");
        B2B.append("#亿商网");
        B2B.append("#商虎中国");
        B2B.append("#志趣网");
        B2B.append("#商务圈");
        B2B.append("#产品网");
        B2B.append("#商通网");
        B2B.append("#谋思网");
        B2B.append("#海商网");
        B2B.append("#中国企业第一网");
        B2B.append("#满分企业网");
        B2B.append("#中麦网");
        B2B.append("#求购信息网");
        B2B.append("#金安发");
        B2B.append("#云商网");
        B2B.append("#创业邦");
        B2B.append("#E路网");
        B2B.append("#比途黄页网中国供应商");
        B2B.append("#天助网");
        B2B.append("#企汇网");
        B2B.append("#百页网");
        B2B.append("#仪器仪表交易网");
        B2B.append("#网商之窗");
        B2B.append("#企业谷");
        B2B.append("#一呼百应");
        B2B.append("#云同盟");
        B2B.append("#招商网");
        B2B.append("#免费发布网");
        B2B.append("#金泉网");
        B2B.append("#88订单网");
        B2B.append("#我帮网");
        B2B.append("#千金商务网");
        B2B.append("#中国企业链");
        B2B.append("#零距离商务网");
        B2B.append("#安装信息网");
        B2B.append("#赛门国际");
        B2B.append("#中国贸易网");
        B2B.append("#慧聚中国");
        B2B.append("#品牌网");
        B2B.append("#一线网");
        B2B.append("#淘金地");
        B2B.append("#易龙商务网");
        B2B.append("#顺企网");
        B2B.append("#百方网");
        B2B.append("#企领网");
        B2B.append("#商名网");
    }

    public static StringBuilder O2O = new StringBuilder();

    static {
        O2O.append("万行商业城");
        O2O.append("#本地搜");
    }

    public static StringBuilder czmh = new StringBuilder();

    static {
        czmh.append("谷瀑环保");
        czmh.append("#制冷大市场");
        czmh.append("#维库仪器仪表网");
        czmh.append("#模具企业联盟");
        czmh.append("#商业机器人");
        czmh.append("#纺织网");
        czmh.append("#九正建材网");
        czmh.append("#化工产品网");
        czmh.append("#机械在线");
        czmh.append("#食品商务网");
        czmh.append("#管道企业库");
        czmh.append("#全球机械网");
        czmh.append("#中国木业网");
        czmh.append("#中国数控机床网");
        czmh.append("#万贯五金机电网");
        czmh.append("#天成医疗网");
        czmh.append("#青青花木网");
        czmh.append("#中国金属网");
        czmh.append("#世界工厂");
        czmh.append("#机电在线");
        czmh.append("#中国自动化网");
        czmh.append("#全球塑胶网");
        czmh.append("#中国工程网");
        czmh.append("#建材采购网");
        czmh.append("#中服网");
        czmh.append("#中国园林网");
        czmh.append("#中国管道商务网");
        czmh.append("#中国电动车网");
        czmh.append("#化工仪器网");
        czmh.append("#中国五金商机网");
        czmh.append("#中国服装网");
        czmh.append("#中玻网");
        czmh.append("#化工机械网");
        czmh.append("#维库电子市场网");
        czmh.append("#太平洋安防网");
        czmh.append("#华强电子网");
        czmh.append("#药品资讯网");
        czmh.append("#制造交易网");
        czmh.append("#建材网");
        czmh.append("#中国工业电器网");
        czmh.append("#十大品牌网");
        czmh.append("#铝道网");
        czmh.append("#五金机电网");
    }

    public static StringBuilder map = new StringBuilder();

    static {
        map.append("城市吧地图");
    }

    public static StringBuilder nianbao = new StringBuilder();

    static {
        nianbao.append("年报");
        nianbao.append("#2013年报");
        nianbao.append("#2014年报");
        nianbao.append("#2015年报");
        nianbao.append("#2016年报");
        nianbao.append("#2017年报");
        nianbao.append("#2018年报");
        nianbao.append("#2019年报");
    }

    public static StringBuilder qiye = new StringBuilder();

    static {
        qiye.append("勤财黄页网");
        qiye.append("#找找去");
        qiye.append("#天眼查");
        qiye.append("#利库搜黄页网");
        qiye.append("#华企黄页网");
        qiye.append("#国家企业信用信息公示系统");
        qiye.append("#启信宝");
        qiye.append("#企业信息网");
        qiye.append("#黄页88");
        qiye.append("#企查猫");
        qiye.append("#企业在线");
        qiye.append("#厂商黄页网");
        qiye.append("#爱启查公众号");
        qiye.append("#百度企业信用");
        qiye.append("#浙江民营企业网");
        qiye.append("#企业录");
        qiye.append("#园林黄页");
        qiye.append("#爱企查公众号");
        qiye.append("#钉钉");
        qiye.append("#水滴信用");
        qiye.append("#中国企业大黄页");
    }

    public static StringBuilder shfw = new StringBuilder();

    static {
        shfw.append("查电话");
    }

    public static StringBuilder zhaobiao = new StringBuilder();

    static {
        zhaobiao.append("阿德采购网");
    }

    public static StringBuilder zhaopin = new StringBuilder();

    static {
        zhaopin.append("宁夏英才网");
        zhaopin.append("康强医疗人才网");
    }

    public static StringBuilder ziliao = new StringBuilder();

    static {
        ziliao.append("中国报告大厅");
        ziliao.append("中国瞪羚");
    }

    public static StringBuilder zixun = new StringBuilder();

    static {
        zixun.append("007商务站");
    }

    public static StringBuilder qita = new StringBuilder();

    static {
        qita.append("其他");
        qita.append("初始导入");
    }
}
