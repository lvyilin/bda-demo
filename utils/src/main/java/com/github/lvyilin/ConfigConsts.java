package com.github.lvyilin;

import java.nio.file.Paths;


public class ConfigConsts {
    public static String DATA_PATH; /*数据集路径*/

    static {
        DATA_PATH = Paths.get(System.getProperty("user.dir"), "res", "data_head200.txt").toString();
    }

}
