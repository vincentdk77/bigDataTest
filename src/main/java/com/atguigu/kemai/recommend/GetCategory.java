package com.atguigu.kemai.recommend;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.kemai.utils.Cosine;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class GetCategory {
    public static void main(String args[]) {

//        try {
//            File file = new File("./src/main/scala/com/kemai/recommend/category.json");
//
//            FileReader  fr = new FileReader(file);
//            BufferedReader br = new BufferedReader(fr);
//            String line  = "";
//            List<CategoryItem> list = new ArrayList<CategoryItem>();
//            while((line = br.readLine()) != null) {
//                JSONObject json = JSONObject.parseObject(line);
//                String id = json.getString("id");
//                String name = json.getString("name");
//                CategoryItem item = new CategoryItem();
//                item.setId(id);
//                item.setName(name);
//                list.add(item);
//            }
//            List<CategoryItem> treeList= new ArrayList<CategoryItem>();
//            for(CategoryItem ci : list) {
//                String id = ci.getId();
//                if(id.length() == 2) {
//                    treeList.add(ci);
//                }
//            }
//            createTree(treeList,list,2);
//            printTree(treeList,"");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        String op = "销售食品饮料酒日用百货家用电器装饰材料服装针纺织品;";
//        System.out.println(getSimilarCategory(op).getName());
    }

    private static void printTree(List<CategoryItem> treeList, String preName) {
        if (treeList != null && treeList.size() > 0) {
            for (CategoryItem ci : treeList) {
                String name = ci.getName();
                String currentName = "";
                if (StringUtils.isEmpty(preName)) {
                    currentName = name;
//                    System.out.println(name);
                } else {
                    currentName = preName + "," + name;
                    if (currentName.split(",").length == 3) {
                        System.out.println(ci.getId() + "aa" + currentName);
                    }
                }

                if (ci.getChilds() != null) {
                    printTree(ci.getChilds(), currentName);
                }
            }
        }

    }

    private static void createTree(List<CategoryItem> treeList, List<CategoryItem> list, int level) {
        for (CategoryItem tl : treeList) {
            List<CategoryItem> childList = new ArrayList<CategoryItem>();
            for (CategoryItem ci : list) {
                String id = ci.getId();
                if (id.startsWith(tl.getId()) && id.length() == level + 1) {
                    childList.add(ci);
                }
            }
            createTree(childList, list, level + 1);
            tl.setChilds(childList);
        }
    }

    public static ArrayList<CategoryItem> getFinalCategoryList() {

        ArrayList<CategoryItem> categoryList = new ArrayList<CategoryItem>();

        String[] lines = CategoryStr.s.toString().split("#");
        for (String s : lines) {
            JSONObject category = JSONObject.parseObject(s);
            String id = category.getString("id");
            String pid = category.getString("pId");
            String name = category.getString("name");
            JSONObject parent = category.getJSONObject("parent");

            CategoryItem item = new CategoryItem();
            item.setId(id);
            item.setPid(pid);
            item.setName(name);
            item.setParent(parent);
            categoryList.add(item);
        }

        return categoryList;
    }

    public static CategoryItem getSimilarCategory(ArrayList<CategoryItem> categoryList, String opScore) {
        Double finalSimilar = 0.0;
        CategoryItem finalCategory = new CategoryItem();
        for (CategoryItem category : categoryList) {
            double similarity = Cosine.getSimilarity(category.getName(), opScore);
            if (similarity > finalSimilar) {
                finalCategory = category;
                finalSimilar = similarity;
            }
        }
        return finalCategory;
    }

    public static JSONObject getNew(CategoryItem categoryItem, JSONObject json) {
        JSONObject newJson = new JSONObject();
        for (String key : json.keySet()) {
            newJson.put(key, json.getString(key));
        }
        newJson.put("category", categoryItem.getName());    //??????
        newJson.put("id", categoryItem.getId());            //??????
        return newJson;
    }
}