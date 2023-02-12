from pandas.tseries.offsets import Day
import datetime

Val_Date  = datetime.datetime(2023, 1, 31)
interval = 1
print("================1-30==============")
print(Val_Date + Day(-30+interval),Val_Date)
print("================31-60==============")
print(Val_Date + Day(-60+interval),Val_Date+ Day(-31+interval))
print("================61-90==============")
print(Val_Date + Day(-90+interval),Val_Date+ Day(-61+interval))
##############################################################
##############################################################
print("================91-120==============")
print(Val_Date + Day(-120+interval),Val_Date+ Day(-91+interval))
print("================121-150==============")
print(Val_Date + Day(-150+interval),Val_Date+ Day(-121+interval))
print("================151-180==============")
print(Val_Date + Day(-180+interval),Val_Date+ Day(-151+interval))
##############################################################
print("================181-210==============")
print(Val_Date + Day(-210+interval),Val_Date+ Day(-181+interval))
print("================211-240==============")
print(Val_Date + Day(-240+interval),Val_Date+ Day(-211+interval))
print("================241-270==============")
print(Val_Date + Day(-270+interval),Val_Date+ Day(-241+interval))
##############################################################
print("================271-300==============")
print(Val_Date + Day(-300+interval),Val_Date+ Day(-271+interval))
print("================301-330==============")
print(Val_Date + Day(-330+interval),Val_Date+ Day(-301+interval))
print("================331-360==============")
print(Val_Date + Day(-360+interval),Val_Date+ Day(-331+interval))
##############################################################
print("================>361==============")
print(Val_Date + Day(-361+interval))