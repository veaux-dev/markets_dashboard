# orq_screener.py
import logging
import pandas as pd
from svc import collector, analyzer, screener, db_mgmt, load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    #tickers = ["WMT", "TGT", "COST", "DG", "DLTR", "BBY", "M", "KSS", "JWN", "ROST"]
    tickers = [
"^MXX","1211N.MX","1810N.MX","3690N.MX","9988N.MX","AAL.MX","AAPL.MX","AAXJ.MX",
"ABEVN.MX","ABNB.MX","AC.MX","ACHR.MX","ACNN.MX","ACWV.MX","ADBE.MX","ADSK.MX",
"AEMN.MX","AG1N.MX","AGNC.MX","AGUA.MX","AGUILASCPO.MX","AI1.MX","AIQ.MX",
"ALAB.MX","ALFAA.MX","ALPEKA.MX","ALSEA.MX","AMAT.MX","AMC.MX","AMD.MX",
"AMXB.MX","AMZN.MX","ANET.MX","ANF.MX","ANGELD10.MX","APH.MX","ARA.MX",
"ARGT.MX","ARMN.MX","ASMLN.MX","ASML1N.MX","ASTS.MX","ASURB.MX","AUN.MX",
"AVGO.MX","AXP.MX","AXTELCPO.MX","BN.MX","BA.MX","BABAN.MX","BAC.MX","BBN.MX",
"BB3MN.MX","BBAI.MX","BBAJIOO.MX","BBUS.MX","BBVA.MX","BBWI.MX","BBY.MX",
"BE.MX","BIDUN.MX","BIL.MX","BILIN.MX","BIMBOA.MX","BLDR.MX","BLKCN.MX",
"BMNR.MX","BND.MX","BNGO.MX","BOLSAA.MX","BOTZ.MX","BRKB.MX","BSX.MX",
"BTBTN.MX","BURL.MX","BYND.MX","C.MX","CADUA.MX","CANAN.MX","CAT.MX",
"CBU0N.MX","CBU7N.MX","CCAUN.MX","CCL1N.MX","CDISN.MX","CDNS.MX","CELH.MX",
"CEMEXCPO.MX","CEMQN.MX","CEUUN.MX","CHDRAUIB.MX","CHTR.MX","CHWY.MX",
"CIBR.MX","CIFR.MX","CJPUN.MX","CL.MX","CLF.MX","CLSK.MX","CMCSA.MX",
"CMG.MX","CMRB.MX","CNDXN.MX","COIN.MX","COP.MX","COST.MX","CPNG.MX",
"CPXJN.MX","CRM.MX","CRUS.MX","CRWD.MX","CRWV.MX","CSPXN.MX","CSPXXN.MX",
"CSTPN.MX","CSUSN.MX","CTALPEKA.MX","CTAS.MX","CTAXTELA.MX","CTEC2N.MX",
"CUERVO.MX","CUSSN.MX","CVNA.MX","CVS.MX","DAL.MX","DANHOS13.MX","DAPPN.MX",
"DBN.MX","DDD.MX","DELLC.MX","DEON.MX","DGRAN.MX","DIA.MX","DIABLOI10.MX",
"DIS.MX","DKNG1.MX","DLRTRAC15.MX","DNUT.MX","DOCU.MX","DRIP.MX","DTLAN.MX",
"DUOL.MX","DUST.MX","EA.MX","ECN.MX","ECARN.MX","EDC.MX","EDMUN.MX","EDZ.MX",
"EEFT.MX","EIMIN.MX","ELEKTRA.MX","EMBMXXN.MX","EMCAN.MX","EMGAN.MX","EMHY.MX",
"ENGYN.MX","ENPH.MX","ERNAN.MX","EWS.MX","EXCHN.MX","FAS.MX","FAZ.MX","FCEL.MX",
"FCFE18.MX","FDS.MX","FDX.MX","FEMSAUBD.MX","FEXI21.MX","FHIPO14.MX",
"FI.MX","FIBRAHD15.MX","FIBRAMQ12.MX","FIBRAPL14.MX","FIBRATC14.MX","FIHO12.MX",
"FINN13.MX","FIVE.MX","FLEXN.MX","FLOAN.MX","FMTY14.MX","FMX23.MX","FNCLN.MX",
"FNOVA17.MX","FRAGUAB.MX","FRES.MX","FRMXNXN.MX","FSHOP13.MX","FSLR.MX","FTNT.MX",
"FUBO.MX","FUNO11.MX","FUTUN.MX","GAPB.MX","GAP1.MX","GBMO.MX","GCARSOA1.MX",
"GCC.MX","GDX.MX","GENTERA.MX","GEVO.MX","GFIN.MX","GFINBURO.MX","GFNORTEO.MX",
"GICSAB.MX","GISSAA.MX","GLD.MX","GLDM.MX","GM.MX","GMD.MX","GME.MX","GMEXICOB.MX",
"GMXT.MX","GOOG.MX","GOOGL.MX","GPRO.MX","GRABN.MX","GRAL.MX","GRID.MX","GRMNN.MX",
"GRUMAB.MX","GT.MX","GUSH.MX","HCITY.MX","HERDEZ.MX","HERO.MX","HIMS.MX","HLTHN.MX",
"HMYN.MX","HON.MX","HOOD.MX","HPE.MX","HPQ.MX","HTZ1.MX","HUT1.MX","HWM.MX","HYG.MX",
"I37MXN.MX","I500N.MX","IAU.MX","IAUPN.MX","IB01N.MX","IB1MXXN.MX","IBM.MX","IBTAN.MX",
"IBTMXXN.MX","ICHB.MX","ICHNN.MX","ICLN.MX","ID26MXN.MX","IDTPN.MX","IGLNN.MX",
"IHYAN.MX","IHYMXXN.MX","IJPAN.MX","IJPDN.MX","IMBAN.MX","INDA.MX","INTC.MX","IONQ.MX",
"ISACN.MX","ISFDN.MX","ISRG.MX","IT27MXN.MX","ITECN.MX","IUAAN.MX","IUCDN.MX","IUCSN.MX",
"IUESN.MX","IUFSN.MX","IUHCN.MX","IUISN.MX","IUITN.MX","IUMSN.MX","IUQAN.MX","IUUSN.MX",
"IVV.MX","IVVPESOISHRS.MX","IWMON.MX","IXC.MX","IYW.MX","JDN.MX","JDST.MX","JGHYN.MX",
"JNJ.MX","JNUG.MX","JPEAN.MX","JPM.MX","JU13N.MX","KHC.MX","KIMBERA.MX","KMB.MX",
"KO.MX","KOFUBL.MX","KVUE.MX","KWEB.MX","LABB.MX","LABD.MX","LABU.MX","LACOMERUBC.MX",
"LASITE.MX","LCID.MX","LEN.MX","LIVEPOL1.MX","LIVEPOLC-1.MX","LLY.MX","LMND.MX",
"LMT.MX","LQDAN.MX","LULU.MX","LYFT.MX","MARA.MX","MBILN.MX","MBLY.MX","MCD.MX",
"MCHI.MX","MEDICAB.MX","MEGACPO.MX","MELIN.MX","META.MX","MFRISCOA-1.MX","MNST.MX",
"MOMON.MX","MP.MX","MRK.MX","MRNA.MX","MRVL1.MX","MSFT.MX","MSTR.MX","MTRLN.MX","MU.MX",
"MVOLN.MX","NAFTRACISHRS.MX","NBISN.MX","NCLHN.MX","NDUSN.MX","NEMAKA.MX","NEXT25.MX",
"NFLX.MX","NION.MX","NKE.MX","NRSH.MX","NUN.MX","NUGT.MX","NUTRISAA.MX","NVAX.MX",
"NVDA.MX","NVON.MX","NVTS.MX","O.MX","OCGN.MX","ODFL.MX","OMAB.MX","OPEN1.MX","ORBIA.MX",
"ORCL.MX","OSCR.MX","OXY1.MX","PALL.MX","PANW.MX","PATH.MX","PCG.MX","PEP.MX","PFE.MX",
"PG.MX","PINFRA.MX","PINFRAL.MX","PINS.MX","PLTR.MX","PLUG.MX","PROK.MX","PSQ.MX",
"PTON.MX","PYPL.MX","Q.MX","QCLN.MX","QCOM.MX","QLD.MX","QQQ.MX","QS.MX","QUBT.MX",
"RA.MX","RBLX.MX","RBRK.MX","RCL.MX","RGTI.MX","RIOT.MX","RIVN.MX","RKLB.MX","ROKU.MX",
"RUN.MX","SANN.MX","SASUN.MX","SBET.MX","SBUX.MX","SDIAN.MX","SEDGN.MX","SGOV.MX","SHAK.MX",
"SHLD.MX","SHOPN.MX","SHV.MX","SHY.MX","SIL.MX","SITES1A-1.MX","SLV.MX","SLVP.MX",
"SMARTRC14.MX","SMCI.MX","SMH.MX","SMHUN.MX","SMLR.MX","SMSNN.MX","SNAP.MX","SNOW.MX",
"SNYN.MX","SOFI.MX","SONYN.MX","SORIANAB.MX","SOUN.MX","SOXL.MX","SOXS.MX","SOXX.MX",
"SP20N.MX","SPCE.MX","SPLG.MX","SPMVN.MX","SPORTS.MX","SPXL.MX","SPXS.MX","SPXS1N.MX",
"SPY.MX","SPYG.MX","SPYLN.MX","SQQQ.MX","STLAN.MX","STORAGE18.MX","STYCN.MX","STZ.MX",
"SXR7N.MX","SYM.MX","TALN.MX","TBBBN.MX","TEAKCPO.MX","TECL.MX","TECS.MX","TEM.MX",
"TER.MX","TERRA13.MX","TEVAN.MX","TFRNN.MX","TGT.MX","TIGRN.MX","TIP.MX","TLEVISACPO.MX",
"TLT.MX","TMDX.MX","TMF.MX","TMO.MX","TNA.MX","TOST.MX","TQQQ.MX","TR7AN.MX","TRAXIONA.MX",
"TSLA.MX","TSLL.MX","TSMN.MX","TSN.MX","TTD.MX","TTWO.MX","TWLO.MX","TX.MX","TZA.MX",
"UAA.MX","UBER.MX","UDR.MX","UNH.MX","UPS.MX","UPST.MX","UPWK.MX","URA.MX","UTILN.MX",
"V.MX","VALEN.MX","VASCONI.MX","VDEAN.MX","VDSTN.MX","VDTAN.MX","VEA.MX","VESTA.MX",
"VFEAN.MX","VGK.MX","VGT.MX","VIG.MX","VINTE.MX","VISTAA.MX","VMCAXN.MX","VMEX19.MX",
"VMSTXN.MX","VNQ.MX","VNQI.MX","VNRAN.MX","VOLARA.MX","VOO.MX","VPL.MX","VRT.MX",
"VSCO.MX","VT.MX","VTI.MX","VTIP.MX","VUAAN.MX","VWO.MX","VWRAN.MX","VYM.MX",
"WALMEX.MX","WBD.MX","WFC.MX","WMGTN.MX","WMT.MX","WULF.MX","XLCSN.MX","XLK.MX",
"XLU.MX","XLV.MX","XPEVN.MX","XSD.MX","XYZ.MX","YANG.MX","YINN.MX","ZETA.MX","ZTON.MX"
] # he ahi seniores. toda la bmv. cuanod mas o menos . 

    timeframes = ["1d"]   # arranca solo con daily para probar

    # 1. Inicializar DB
    db_mgmt.init_db()

    # 2. Descargar precios
    logging.info("📥 Descargando precios...")
    intervals_cfg = [{"name":"1d","interval":"1d","period":"20y"}]
    working_db = collector.download_tickers(tickers, intervals_cfg=intervals_cfg)
    

    # 3. Calcular indicadores
    logging.info("📊 Calculando indicadores...")
    working_db = analyzer.analyse_data(working_db)

    prices_cols = ["ticker", "timeframe", "date", "open", "high", "low", "close", "volume"]
    ind_cols = [ #OJO QUE LAS COLUMNAS DEBEN DE IR EN EL MISMO ORDEN QUE SE CREO LA DB
        "ticker", "timeframe", "date",
        "rsi",
        "macd", "macd_signal", "macd_hist",
        "ema_short", "ema_long",
        "bb_upper", "bb_middle", "bb_lower",
        "bb_bandwidth", "bb_percent",
        "adx", "adxr", "di_plus", "di_minus",
        "donchian_high", "donchian_low", "donchian_mid",
        "vol_sma20", "vol_ema20", "obv", "cmf", "mfi"
    ]
    
    all_prices=[]
    all_kpis=[]  

    # 4. Insertar precios e indicadores a DB
    logging.info("📥 Preparing Data for DB Insert...")
    for ticker, tfs in working_db.items():
        for tf, df in tfs.items():
            df = df.reset_index().rename(columns={"date": "date"})
            df["ticker"] = ticker
            df["timeframe"] = tf
            df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

            all_prices.append(df[prices_cols])
            all_kpis.append(df[ind_cols])

            
    df_prices=pd.concat(all_prices,ignore_index=True)
    df_kpis=pd.concat(all_kpis, ignore_index=True)


    logging.info("📥 Cargando Tickers y precios a la base...")
    db_mgmt.insert_prices(df_prices[prices_cols])
    
    logging.info("📥 Cargando Indicadores a la base...")
    db_mgmt.insert_indicators(df_kpis[ind_cols])

    # 5. Correr screener
    logging.info("🚦 Corriendo screener...")
    signals = screener.run_screener(timeframe="1d", all_data= True)

    # 6. Mostrar resultado en consola
    print("\n📋 Screener Results:")
    print(signals)

if __name__ == "__main__":
    main()

