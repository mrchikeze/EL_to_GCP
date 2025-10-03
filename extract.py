import pandas as pd
import glob

def extract():
        
    cf=glob.glob("csv/*.csv")
    csv=pd.concat([pd.read_csv(f) for f in cf], ignore_index=True)

    xf=glob.glob("excel/*.xlsx")
    exc=pd.concat([pd.read_excel(f) for f in xf], ignore_index=True)

    jf=glob.glob("json/*.json")
    js=pd.concat([pd.read_json(f, encoding="latin1", lines=True) for f in jf], ignore_index=True)


    new_df_exc=exc.copy()
    new_df_exc=new_df_exc.drop(columns=["Discount(%)","FinalTotal"])   #selecting needed columns


    all_join_df=pd.concat([js, csv, new_df_exc],ignore_index=True)   # joining all datasource
    all_join_df=all_join_df.drop_duplicates(subset=["OrderID"])   #removing duplicates using the column orderid
    all_join_df=all_join_df.reset_index(drop=True)

    print(cf,jf,xf)

    return all_join_df

