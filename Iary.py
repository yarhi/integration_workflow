#! /usr/bin/env python
# -*- coding: utf8
import os, sys,re,codecs,smtplib,time
from colorama import *
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import pymssql
from openpyxl import Workbook
import datetime
from datetime import date,timedelta
daty = str(date.today()).replace('-','')


class Cnx_DBRay():
	""" class cnx by raymanjune """
	def __init__(self,typ='pg', host='localhost', dbname='Data_interne', user='postgres', password='123456',port='5432',mode_='user'):
		self.hostname = host
		self.dbname = dbname
		self.user = user
		self.password = password
		self.port = port
		self.typ = typ
		self.CrimTx = lambda arr_:list(map(lambda str_:str(str_).replace("'","''").replace('"','').replace('None','').encode('utf8'),arr_))
	def connecting(self):
		nb_tentative = 1
		Object_Aretruned ={}

		if str(self.typ).lower() == 'pg':
			print (Fore.GREEN + " Initialisation cnx postgres encour... #Tentative "+str(nb_tentative))
			try:
			    self.con = psycopg2.connect('host='+self.hostname+' port='+self.port +' dbname='+self.dbname+' user='+self.user+' password='+ self.password)
			    self.con.set_isolation_level(0)
			    self.curseur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
			    #self.con.set_client_encoding('WIN1252') 
			    return True
			except psycopg2.DatabaseError as e:
			    print (Fore.RED + " Impossible de se connecter a {} voici les detail: {}").format(self.hostname,e)
			    return False
		elif str(self.typ.lower() == 'sq'):
			print (Fore.GREEN + " Initialisation cnx sql server encours...")
			try:
			   self.con = pymssql.connect(server=self.hostname,database=self.dbname)   
			   self.curseur=self.con.cursor()
			   return True			   
			except pymssql.DatabaseError as e:
			    print (Fore.RED + " Impossible de se connecter a {} voici les detail: {}").format(self.hostname,e)
			    return False
		else:
			pass
		Object_Aretruned['cnx'] = self.con
		Object_Aretruned['curseur'] = self.curseur
		print (Fore.GREEN + " Connexion etabli avec success ")
		
		return Object_Aretruned

	
	def commiteo(self):
		self.con.commit()
	def closeo(self):
		print (Fore.GREEN + " Fermeture de la connexion ")
		self.con.close()
		time.sleep(2)
		print (Fore.RED + " connexion fermer avec succes ")
		print (Fore.GREEN + " Fermeture du curseur ")		
		self.curseur.close()
		time.sleep(2)
		print (Fore.RED + " Curseur fermer avec success ")
	def nb_records(self, tbname, where='1=1'):
		Sql_count = "Select count(*) from %s where %s"%(tbname, where)
		print (Sql_count)
		try:
			self.curseur.execute(Sql_count)
			return self.curseur.fetchone()[0]
		except Exception as  e:
			print (Fore.RED + " Une erreur est survenue lors de l'execution du req : %s \nvoici les details : \n %s"%(Sql_count,e))
	def execute_crud(self, sql,typ='u'):
		if typ.lower()=='u' or typ.lower==('d') or typ.lower==('i'):
			try:
				self.curseur.execute(sql)
				self.con.commit()				
			except Exception as  e:
				print (Fore.RED + " Une erreur est survenue lors de l'execution du req : %s \nvoici les details : \n %s"%(sql, e))
		else:			
			try:
				self.curseur.execute(sql)
				return self.curseur.fetchall()
			except Exception as  e:
				print (Fore.RED + " Une erreur est survenue lors de l'execution du req : %s \nvoici les details : \n %s"%(sql, e))
	def insert_TbtoTb(self,source_con, UnionChp, tb_destination,tb_source, where='1=1',type_source='pg'):
		print (Fore.GREEN + "Copie de la table " + str(tb_source) + "vers "+ str(tb_destination) + " encours...")
		if type_source.lower() =='pg':		
			FormedSql = "Set client_encoding='utf8';SELECT %s FROM %s WHERE %s "%(UnionChp, tb_source, where)
		else:
			FormedSql = "SELECT %s FROM %s WHERE %s "%(UnionChp, tb_source, where)
		print (FormedSql)
		source_con.execute(FormedSql)
		all_data = source_con.fetchall()
		print (all_data)
		sql_insert = "INSERT INTO %s (%s) "%(tb_destination,UnionChp)
		for data in all_data:
			rqComplet = sql_insert + " VALUES ('" + "','".join(self.CrimTx(data)) + "')"			
			try:
			   	   
			   self.curseur.execute(rqComplet)
			except Exception as  e:
			    print (Fore.RED + " Impossible d'executer {} voici les details: {}").format(rqComplet,e)
			    sys.exit(1)
		self.commiteo()
		print (Fore.YELLOW + " Copier terminer avec success")
	def export_data(self, curs, tb_name, champ='', entete='',where='1=1', filename_='',typecurseur='pg', avec_t ='oui'):
		export_ = Workbook()
		fll = export_.active
		if filename_ == '': filename_ = "export_"+str(str(datetime.datetime.now()).split('.')[0]).replace('-','_').replace(':','_') +'.xlsx'
		if champ == '':
			if str(typecurseur).lower() == 'pg':				
				recolt_champ = "SELECT * FROM %s LIMIT 0 "%(tb_name)
				curs.execute(recolt_champ)
				chps = [cols[0] for cols in curs.description]
				chps_select = ",".join(chps)
				req = "Set client_encoding='utf8';SELECT %s FROM %s WHERE %s"%(chps_select, tb_name,where)
			else:
				recolt_champ = "SELECT top 0 * FROM %s "%(tb_name)
				curs.execute(recolt_champ)
				chps = [cols[0] for cols in curs.description]
				chps_select = ",".join(chps)
				req = "SELECT %s FROM %s WHERE %s"%(chps_select, tb_name,where)
		else:
			chps_select = champ			
			if str(typecurseur).lower() == 'pg':				
				req = "Set client_encoding='utf8';SELECT %s FROM %s WHERE %s"%(chps_select, tb_name,where)
			else:
				req = "SELECT %s FROM %s WHERE %s"%(chps_select, tb_name,where)
		curs.execute(req)
		datas = curs.fetchall()
		
		if str(avec_t) == 'oui':			
			if entete !='':
				fll.append(entete)
			else:
				fll.append(chps)
			for data in datas:
				fll.append(data)			
			export_.save(filename_)
		else:
			for data in datas:
				fll.append(data)			
			export_.save(filename_)
	def vider_table(self, tb_name):
		req = "DELETE FROM %s " %(tb_name)
		try:		
			self.curseur.execute(req)
		except Exception as  e:
			print ("Une erreur est survenue lors de suppression des données:\n{}".format(e))
		self.commiteo()
			
	def insertion_(self, array_col, datas, tb_name):
		values = " INSERT INTO "+str(tb_name)+"("+",".join(array_col)+") VALUES ('"		
		for data in datas:			
			list_element = "','".join(list(self.CrimTx(data)))+ "')"
			rec_complet = values + list_element
			try:
				self.curseur.execute(rec_complet)
			except Exception as  e:
				print ("une erreur est survenue lors de l'insertion de votre donnes:\n{}".format(e))
		self.commiteo()
	def GenRal(self,array_valeur):
		for elem in array_valeur:
			yield "'" + "','".join(self.CrimTx(elem)) + "'"

	def insertion_QTb(self,sql,chps, tb_destination='TMP_NOMINATION_ETL_FICHE'):
		try:
			
			self.curseur.execute(sql)
			data_fiche = self.curseur.fetchall()
			for enr in self.GenRal(data_fiche):
				sql_insert = "INSERT INTO {}({})VALUES({})".format(tb_destination,chps,enr)
				try:
					print (sql_insert)                   
					self.curseur.execute(sql_insert)
				except pymssql.DatabaseError as e:
					print ("# Erreur lors de l'insertion : {}".format(e))
					return False
			self.commiteo()
		except Exception as e:
			print ("erreur ato: {}".format(e))
			return False
	                   
	
	def selectOne(self, sql):
		self.curseur.execute(sql)
		return self.curseur.fetchone()
	def select_date_int(self,for_this_date):
		simple_qry = "SELECT SK_date_si FROM  [DWH_BILLING].[dbo].dwh_dim_date WHERE CONVERT(VARCHAR(10), date_d, 120) ='{}'".format(for_this_date)
		return self.selectOne(simple_qry)
	def return_mssql_dict(self,sql):
	    try:
	        self.curseur.execute(sql)
	        def return_dict_pair(row_item):
	            return_dict = {}
	            for column_name, row in zip(self.curseur.description, row_item):
	                return_dict[column_name[0]] = row
	            return return_dict
	        return_list = []
	        for row in self.curseur:
	            row_item = return_dict_pair(row)
	            return_list.append(row_item)
	        return return_list
	    except Exception as e:
	        print(e)

if __name__ == '__main__':
	print("try back later !!!")
	
	# anio =  datetime.date.today()
	# week_info = {}
	# weekly_info = {}

	# def check_if_monday(date_):
	# 	year, month, day = (int(x) for x in str(date_).split('-'))    
	# 	jour = datetime.date(year, month, day)
	# 	if jour.strftime("%A") == 'Monday':
	# 		print("c'est lundi ")
	# 		return True
	# 	print("impossible de lance le programme parce que aujourd'hui n'est pas lundi")
	# 	return False
			
	# if check_if_monday('2021-01-04'):
	# 	from dateutil.parser import parse
	# 	anio = parse('2021-01-04')
	# 	week_info['end_of_last_week'] = str(anio - timedelta(days=1)).split(' ')[0]
	# 	week_info['start_of_last_week'] = str(anio - timedelta(days=7)).split(' ')[0]
	# else:

	# 	sys.exit()

	# print(week_info)

	# TDBP78WV = Cnx_DBRay(typ='sq',host='TDBP78WV',dbname='dwh_billing')
	# TDBP21WV = Cnx_DBRay(typ='sq',host='TDBP21WV',dbname='dmkt_reporting') 
	# destination = Cnx_DBRay(typ='pg')
	# weekly_info.update({"week": week_info['start_of_last_week'] +" - " + week_info['end_of_last_week'], "week_number":parse(week_info['start_of_last_week']).isocalendar()[1] })
	
	# if destination.connecting() and	TDBP78WV.connecting() and TDBP21WV.connecting():
	# 	#get all info in parametrage
	# 	print ('connexion reussis')
	# 	sql_ = """
	# 	SELECT * FROM parametrage WHERE flag='1'
	# 	"""
	# 	data_param = destination.return_mssql_dict(sql_)
	# 	for element in data_param:
	# 		source = {'source_': element['server_name']}
	# 		print(source['source_'])
	# 		if element['variable_type'] =='dateInt':
	# 			datein_min = eval(source['source_']).select_date_int(week_info['start_of_last_week'])[0]
	# 			datein_max = eval(source['source_']).select_date_int(week_info['end_of_last_week'])[0]
	# 		else:
	# 			datein_min = week_info['start_of_last_week']
	# 			datein_max = week_info['end_of_last_week']
	# 		query_ = element['query_to_use']
	# 		query_ = query_.format(datein_min=datein_min,datein_max=datein_max)
	# 		print(query_)
	# 		test = eval(source['source_']).return_mssql_dict(query_)
	# 		weekly_info.update(test[0])
	# 	weekly_info.update({'pourcentage_certification': weekly_info.get('certifications',0)/weekly_info.get('GrossAds',1)})	
	# 	print(weekly_info)
	# 	destination.closeo()
	# 	TDBP21WV.closeo()
	# 	TDBP78WV.closeo()
	# 	return True
		
	# else:
	# 	print('ko')
	# 	return False