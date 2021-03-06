﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.ServiceModel.Description;
using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using System.Data;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;
using System.Threading;

namespace MigracionCRMX
{
    class Program
    {
        private static String ErrorlineNo, Errormsg, extype, ErrorLocation;

        static void Main(string[] args)
        {

            #region Funciones principales
            OrganizationService CRM = ConexionCRM();

            // enviarCuentas();
            // enviarContactos();
            // asignarContactoPrimario();
            // enviarNotas(CRM);
            // DeleteRecords(CRM);
            // Console.WriteLine("operacion completa pres enter pls");
            // Console.ReadKey();
            // enviarActividades(CRM, "1", 4202);
            // enviarlistasdemkt(CRM);
            // enviarOportunidades(CRM);
            enviarcampanias(CRM);
            

            #endregion

            #region Pasos de Referencias de Cuentas y Contactos
            //getRelacionNegocio(CRM);
            //getCodigoSic(CRM);
            //getCodigoSubsic(CRM);
            //getCompetidores(CRM);
            //getPaises(CRM);
            //getEstados(CRM);
            //getFabricanteSW(CRM);
            //getSWERP(CRM);
            //getSWCRM(CRM);
            //getTitulosContacto(CRM);
            //getDepartamentosContacto(CRM);
            //
            #endregion
        }

        #region Funciones 
        private static void enviarcampanias(OrganizationService CRM)
        {
            int Total = 0, Actual = 0, Errores = 0;
            String Query = "select top 10 OwnerIdName, * from Campaign as campania inner join SystemUser as propietario on campania.OwnerId = propietario.SystemUserId order by campania.CreatedOn asc";
            DataTable datos = EjecutaQuery(Query);
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Console.WriteLine("Registro: " + Actual + " de " + Total + " Lista: " + item["Name"].ToString() + " de " + item["OwnerIdName"].ToString());
                Campaign campania = new Campaign();
                campania.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["CampaignId"].ToString()}};
                campania.IsTemplate = Boolean.Parse(item["IsTemplate"].ToString());
                campania.ExpectedRevenue = new Money(decimal.Parse(item["ExpectedRevenue"].ToString()));
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                if (existepropietario) { campania.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } //else { campania.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                campania.Name = item["Name"].ToString();
                campania.CodeName = item["CodeName"].ToString();
                campania.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                campania.TypeCode = new OptionSetValue(int.Parse(item["TypeCode"].ToString()));
                if (item["ExpectedResponse"].ToString() != "") campania.ExpectedResponse = int.Parse(item["ExpectedResponse"].ToString());
                if (item["ProposedStart"].ToString()!="") campania.ProposedStart = DateTime.Parse(item["ProposedStart"].ToString());
                if (item["ProposedEnd"].ToString() != "") campania.ProposedEnd = DateTime.Parse(item["ProposedEnd"].ToString());
                if (item["ActualStart"].ToString() != "") campania.ActualStart = DateTime.Parse(item["ActualStart"].ToString());
                if (item["ActualEnd"].ToString()!="") campania.ActualEnd = DateTime.Parse(item["ActualEnd"].ToString());
                if (item["OtherCost"].ToString()!="") campania.OtherCost = new Money(decimal.Parse(item["OtherCost"].ToString()));
                if (item["BudgetedCost"].ToString()!="") campania.BudgetedCost = new Money(decimal.Parse(item["BudgetedCost"].ToString()));
                if (item["CreatedOn"].ToString()!="") campania.OverriddenCreatedOn = DateTime.Parse(item["CreatedOn"].ToString());
                if (item["new_Clasificacion"].ToString()!="") campania.new_Clasificacion = new OptionSetValue(int.Parse(item["new_Clasificacion"].ToString()));
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = campania
                    };
                    UpsertResponse response = (UpsertResponse)CRM.Execute(request);
                    if (response.RecordCreated)
                    {
                        Console.WriteLine("Registro Creado... comprobando estatus ");
                        
                    }
                    else
                    {
                        
                        Console.WriteLine("Registro Actualizado!");
                    }
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["Name"].ToString()+" "+item["CampaignId"].ToString(), item["FullName"].ToString(), "Campanias_MKT");
                    Console.WriteLine("Error!");
                    continue;

                }


            }

        }
        private static void enviarlistasdemkt( OrganizationService CRM)
          {
              int Total = 0, Actual = 0, Errores = 0;
              String Query = "select top 10 * from list as lista inner join SystemUser as propietario on lista.OwnerId = propietario.SystemUserId order by lista.CreatedOn asc  ";
              DataTable datos = EjecutaQuery(Query);
              Total = datos.Rows.Count;
              foreach (DataRow item in datos.Rows)
              {
                  Actual++;
                  Console.WriteLine("Registro: " + Actual + " de " + Total + " Lista: " + item["ListName"].ToString() + " de " + item["OwnerIdName"].ToString());
                  List lista = new List();
                  bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                  lista.KeyAttributes = new KeyAttributeCollection { { "new_clavedelista", item["ListId"].ToString() } };
                  if (existepropietario) { lista.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { lista.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                  lista.ListName = item["ListName"].ToString();
                  lista.Type = bool.Parse(item["Type"].ToString());
                  lista.MemberType = int.Parse(item["MemberType"].ToString());
                  lista.Purpose = item["Purpose"].ToString();
                  if (item["CreatedOn"].ToString() != "") lista.OverriddenCreatedOn = DateTime.Parse(item["CreatedOn"].ToString()); else lista.OverriddenCreatedOn = null;
                  if (item["Cost"].ToString() != "") lista.Cost = new Money(Decimal.Parse(item["Cost"].ToString()));
                  lista.Description = item["Description"].ToString();
                  lista.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null; ;
                  lista.DoNotSendOnOptOut =bool.Parse(item["DoNotSendOnOptOut"].ToString());
                  //lista.new_campania = ;
                  lista.IgnoreInactiveListMembers = bool.Parse(item["IgnoreInactiveListMembers"].ToString());
                  lista.Source = item["Source"].ToString();
                  lista.CreatedFromCode = new OptionSetValue(int.Parse(item["CreatedFromCode"].ToString()));
                #region agregar a los integrantes de la lista
                /*
                String Query2 = "select entityid from FilteredListMember where listid ='" +item["listid"] +"'";
                  DataTable datos2 = EjecutaQuery(Query2);
                 foreach (DataRow item2 in datos2.Rows)
                 {
                    AddListMembersListRequest agregarmiembros = new AddListMembersListRequest
                    {
                        MemberIds = new[] { new Guid(item2["entityid"].ToString()) },
                        ListId = new Guid(item2["listid"].ToString())
                    };
                 }
                 */
                #endregion
                try
                {
                      UpsertRequest request = new UpsertRequest()
                      {
                          Target = lista
                      };
                      UpsertResponse response=(UpsertResponse)CRM.Execute(request);
                    if (response.RecordCreated)
                    {
                        Console.WriteLine("Registro Creado... comprobando estatus ");
                        /*lista.KeyAttributes = new KeyAttributeCollection { { "new_clavedelista", item["ListId"].ToString() } };
                        int estado = Convert.ToInt32(item["StateCode"].ToString());
                        switch (estado)
                        {
                            case 0:
                                lista.StateCode = ListState.Active; lista.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                break;
                            case 1:
                                lista.StateCode = ListState.Inactive; lista.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                break;
                        }
                        UpsertRequest request2 = new UpsertRequest()
                        {
                            Target = lista
                        };
                        UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);
                        Console.WriteLine("Registro (estatus) Actualizado!");*/
                        Console.ReadKey();
                    }
                    else
                    {
                        /*lista.KeyAttributes = new KeyAttributeCollection { { "new_clavedelista", item["ListId"].ToString() } };
                        int estado = Convert.ToInt32(item["StateCode"].ToString());
                        switch (estado)
                        {
                            case 0:
                                lista.StateCode = ListState.Active; lista.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                break;
                            case 1:
                                lista.StateCode = ListState.Inactive; lista.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                break;
                        }
                        UpsertRequest request2 = new UpsertRequest()
                        {
                            Target = lista
                        };
                        UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);*/
                        Console.WriteLine("Registro Actualizado!");
                        Console.ReadKey();
                    }
                  }
                  catch (Exception e)
                  {
                      Errores++;
                      SendErrorToText(e, "Error: " + Errores + " . " + item["ListName"].ToString(), item["FullName"].ToString(), "Listas_MKT");
                    Console.WriteLine("Error!");
                    Console.ReadKey();
                      continue;

                  }


              }

          }
        private static void enviarOportunidades(OrganizationService CRM)
        {
            String Query = "select oportunidad.*,propietario.InternalEMailAddress, propietario.FullName as propietario from Opportunity oportunidad inner join SystemUser as propietario on oportunidad.ownerid = propietario.SystemUserId";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            int Errores = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Console.WriteLine("Registro " + Actual + " de " + Total + ":" + item["Name"].ToString());
                Opportunity Oportunidad = new Opportunity();
                int estado = Convert.ToInt32(item["StateCode"].ToString());
                /* switch (estado)
                 {
                     case 0:
                         Oportunidad.StateCode = OpportunityState.Open; Oportunidad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                         break;
                     case 1:
                         Oportunidad.StateCode = OpportunityState.Won; Oportunidad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                         break;
                     case 2:
                         Oportunidad.StateCode = OpportunityState.Lost; int statsnmbr = Convert.ToInt32(item["StatusCode"].ToString()); switch (statsnmbr) { case 200000: Oportunidad.StatusCode = Oportunidad.StatusCode = new OptionSetValue(100000005); break; case 200001: Oportunidad.StatusCode = Oportunidad.StatusCode = new OptionSetValue(100000006); break; case 200002: Oportunidad.StatusCode = Oportunidad.StatusCode = new OptionSetValue(100000007); break; case 200003: Oportunidad.StatusCode = Oportunidad.StatusCode = new OptionSetValue(100000008); break; default: Oportunidad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString())); break; } 
                         break;
                 }*/

                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                if (existepropietario) { Oportunidad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { Oportunidad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                Oportunidad.KeyAttributes = new KeyAttributeCollection { { "new_claveonpremise", item["OpportunityId"].ToString() } };
                Oportunidad.Name = item["Name"].ToString();
                Oportunidad.ParentAccountId = (item["ParentAccountId"].ToString() != "") ? (new EntityReference(Account.EntityLogicalName, "accountnumber", item["ParentAccountId"].ToString())) : null;
                Oportunidad.new_subtipodelicencias = (item["new_subtipodelicencias"].ToString() != "") ? (new OptionSetValue(int.Parse(item["new_subtipodelicencias"].ToString()))) : null;
                Oportunidad.new_Equipo = (item["new_Equipo"].ToString() != "") ? new OptionSetValue(int.Parse(item["new_Equipo"].ToString())) : null;
                Oportunidad.new_descripser = item["new_descripser"].ToString();
                Oportunidad.new_status = (item["new_status"].ToString() != "") ? new OptionSetValue(int.Parse(item["new_status"].ToString())) : null;

                if (item["EstimatedCloseDate"].ToString() != "") { Oportunidad.EstimatedCloseDate = DateTime.Parse(item["EstimatedCloseDate"].ToString()).AddHours(-5); }
                if (item["CloseProbability"].ToString() != "") { Oportunidad.CloseProbability = int.Parse(item["CloseProbability"].ToString()); }
                Oportunidad.OpportunityRatingCode = (item["OpportunityRatingCode"].ToString() != "") ? new OptionSetValue(int.Parse(item["OpportunityRatingCode"].ToString())) : null;
                Oportunidad.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = Oportunidad
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Oportunidad" + item["Name"].ToString() + " actualizado con exito ");
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["Name"].ToString() + " id " + item["OpportunityId"].ToString(), item[""].ToString(), "Oportunidades");
                    continue;

                }
            }

        }
        private static void enviarActividades(OrganizationService CRM, string objectidentificador, int tipoactividad)
        {
            int Total = 0, Actual = 0, Errores = 0;
            switch (tipoactividad)
            {
                case 4210:
                    #region llamadas
                    String Query = "select * from PhoneCall as actividad inner join SystemUser as propietario on actividad.ownerid = propietario.SystemUserId where actividad.regardingobjecttypecode=" + objectidentificador + "order by actividad.createdon asc";
                    DataTable datos = EjecutaQuery(Query);
                    Total = datos.Rows.Count;
                    foreach (DataRow item in datos.Rows)
                    {
                        Actual++;
                        Console.WriteLine("Registro: " + Actual + " de " + Total + " Llamada a " + item["RegardingObjectIdName"].ToString() + " " + item["Subject"].ToString() + " " + item["OwnerIdName"]);
                        PhoneCall actividad = new PhoneCall();
                        bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                        if (existepropietario) { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                        actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                        actividad.Subject = item["Subject"].ToString();
                        if (item["ActualDurationMinutes"].ToString() != "") actividad.ActualDurationMinutes = Convert.ToInt32(item["ActualDurationMinutes"].ToString());
                        if (item["ActualEnd"].ToString() != "") actividad.ActualEnd = DateTime.Parse(item["ActualEnd"].ToString());
                        if (item["ActualStart"].ToString() != "") actividad.ActualStart = DateTime.Parse(item["ActualStart"].ToString());
                        actividad.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                        actividad.Description = item["Description"].ToString();
                        if (item["CreatedOn"].ToString() != "") { DateTime fecha_creacion = DateTime.Parse(item["CreatedOn"].ToString()); actividad.OverriddenCreatedOn = fecha_creacion.AddHours(-5); } else { actividad.OverriddenCreatedOn = null; }
                        actividad.DirectionCode = bool.Parse(item["DirectionCode"].ToString());
                        actividad.IsBilled = bool.Parse(item["DirectionCode"].ToString());
                        actividad.IsWorkflowCreated = bool.Parse(item["IsWorkflowCreated"].ToString());
                        if (item["ScheduledStart"].ToString() != "") { actividad.ScheduledStart = DateTime.Parse(item["ScheduledStart"].ToString()).AddHours(-5); }
                        if (item["ScheduledEnd"].ToString() != "") { actividad.ScheduledEnd = DateTime.Parse(item["ScheduledEnd"].ToString()).AddHours(-5); }
                        actividad.PhoneNumber = item["PhoneNumber"].ToString();
                        actividad.RegardingObjectId = new EntityReference(Account.EntityLogicalName, "accountnumber", item["RegardingObjectId"].ToString());

                        if (asignarparties(item["ActivityId"].ToString()) != "")
                        {
                            ActivityParty CallFrom = new ActivityParty
                            {
                                PartyId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", asignarparties(item["ActivityId"].ToString()))
                            };
                            actividad.From = new ActivityParty[] { CallFrom };
                        }
                        ActivityParty CallTo = new ActivityParty
                        {
                            PartyId = new EntityReference(Account.EntityLogicalName, "accountnumber", item["RegardingObjectId"].ToString())
                        };
                        actividad.To = new ActivityParty[] { CallTo };
                        if (item["new_Niveldeinteres"].ToString() != "") actividad.new_Niveldeinteres = new OptionSetValue(int.Parse(item["new_Niveldeinteres"].ToString()));
                        actividad.new_TelefonoAlternativo = item["new_TelefonoAlternativo"].ToString();
                        actividad.new_TelefonoParticular = item["new_TelefonoParticular"].ToString();
                        actividad.new_OtroTelefono = item["new_OtroTelefono"].ToString();
                        actividad.new_TelefonoMovil = item["new_TelefonoMovil"].ToString();
                        actividad.new_Extension = item["new_Extension"].ToString();
                        if (item["new_cdigoderespuesta"].ToString() != "") actividad.new_cdigoderespuesta = new OptionSetValue(int.Parse(item["new_cdigoderespuesta"].ToString()));
                        if (item["new_Fechadellamada"].ToString() != "") actividad.new_Fechadellamada = DateTime.Parse(item["new_Fechadellamada"].ToString());
                        try
                        {
                            UpsertRequest request = new UpsertRequest()
                            {
                                Target = actividad
                            };
                            UpsertResponse response = (UpsertResponse)CRM.Execute(request);
                            #region actualizar despues de crear
                            if (response.RecordCreated)
                            {
                                Console.WriteLine("Actividad  Creada... revisando estatus ");
                                // search for an update of status and other attributes
                                /*actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                                int estado = Convert.ToInt32(item["StateCode"].ToString());
                                switch (estado)
                                {
                                    case 0:
                                        actividad.StateCode = PhoneCallState.Open; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 1:
                                        actividad.StateCode = PhoneCallState.Completed; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 2:
                                        actividad.StateCode = PhoneCallState.Canceled; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                }
                                UpsertRequest request2 = new UpsertRequest()
                                {
                                    Target = actividad
                                };
                                UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);
                                Console.WriteLine("Actividad Actualizada!");*/
                            }
                            else
                            {
                                /*actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                                int estado = Convert.ToInt32(item["StateCode"].ToString());
                                switch (estado)
                                {
                                    case 0:
                                        actividad.StateCode = PhoneCallState.Open; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 1:
                                        actividad.StateCode = PhoneCallState.Completed; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 2:
                                        actividad.StateCode = PhoneCallState.Canceled; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                }
                                UpsertRequest request2 = new UpsertRequest()
                                {
                                    Target = actividad
                                };
                                UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);*/
                                Console.WriteLine("Actividad Actualizada!");
                            }


                        }
                        #endregion
                        catch (Exception e)
                        {
                            Errores++;
                            SendErrorToText(e, "Error: " + Errores + " . " + item["activitytypecode"].ToString(), item["activityid"].ToString(), "Llamadas");
                            continue;

                        }

                    }
                    break;
                #endregion
                case 4212:
                    string task = "";
                    break;
                case 4202:
                    #region Emails
                    Query = "select top 100 * from Email as actividad inner join SystemUser as propietario on actividad.ownerid = propietario.SystemUserId where actividad.regardingobjecttypecode=" + objectidentificador + "order by actividad.createdon asc";
                    datos = EjecutaQuery(Query);
                    Total = datos.Rows.Count;
                    foreach (DataRow item in datos.Rows)
                    {
                        Actual++;
                        Console.WriteLine("Registro: " + Actual + " de " + Total + " Email  a " + item["RegardingObjectIdName"].ToString() +" "+ item["OwnerIdName"].ToString() + item["Subject"].ToString());
                        Email actividad = new Email();
                        bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                        if (existepropietario) { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                        actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                        actividad.Subject = item["Subject"].ToString();
                        if (item["ActualDurationMinutes"].ToString() != "") actividad.ActualDurationMinutes = Convert.ToInt32(item["ActualDurationMinutes"].ToString());
                        if (item["ActualEnd"].ToString() != "") actividad.ActualEnd = DateTime.Parse(item["ActualEnd"].ToString());
                        if (item["ActualStart"].ToString() != "") actividad.ActualStart = DateTime.Parse(item["ActualStart"].ToString());
                        actividad.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                        actividad.Description = item["Description"].ToString();
                        if (item["CreatedOn"].ToString() != "") { DateTime fecha_creacion = DateTime.Parse(item["CreatedOn"].ToString()); actividad.OverriddenCreatedOn = fecha_creacion.AddHours(-5); } else { actividad.OverriddenCreatedOn = null; }
                        actividad.DirectionCode = bool.Parse(item["DirectionCode"].ToString());
                        actividad.IsBilled = bool.Parse(item["DirectionCode"].ToString());
                        actividad.IsWorkflowCreated = bool.Parse(item["IsWorkflowCreated"].ToString());
                        if (item["ScheduledStart"].ToString() != "") { actividad.ScheduledStart = DateTime.Parse(item["ScheduledStart"].ToString()).AddHours(-5); }
                        if (item["ScheduledEnd"].ToString() != "") { actividad.ScheduledEnd = DateTime.Parse(item["ScheduledEnd"].ToString()).AddHours(-5); }
                        actividad.ReadReceiptRequested = bool.Parse(item["ReadReceiptRequested"].ToString());
                        actividad.RegardingObjectId = new EntityReference(Account.EntityLogicalName, "accountnumber", item["RegardingObjectId"].ToString());
                        actividad.Subcategory = item["Subcategory"].ToString();
                        actividad.PriorityCode = new OptionSetValue(int.Parse(item["PriorityCode"].ToString()));
                        actividad.TrackingToken = item["TrackingToken"].ToString();
                        actividad.Sender= item["Sender"].ToString();
                        actividad.ToRecipients = item["ToRecipients"].ToString();
                        actividad.MessageId = item["MessageId"].ToString();
                        if(item["DeliveryAttempts"].ToString()!="") actividad.DeliveryAttempts = int.Parse(item["DeliveryAttempts"].ToString());
                        if (item["DeliveryPriorityCode"].ToString() != "") actividad.DeliveryPriorityCode = new OptionSetValue( int.Parse(item["DeliveryPriorityCode"].ToString()));
                        String Query2 = "select participationtypemask as tipo, participationtypemaskname as tiponame,partyobjecttypecode as tipoparty,partyid as guidparty, usuarioparty.InternalEMailAddress as idparty from FilteredActivityParty parties inner join SystemUser as usuarioparty on parties.partyid= usuarioparty.SystemUserId  where ActivityId = '" + item["ActivityId"].ToString() + "'";
                        DataTable datos2 = EjecutaQuery(Query2);
                        foreach (DataRow item2 in datos2.Rows)
                        {
                            if (item2["tipo"].ToString() == "1")
                            {
                                ActivityParty From = new ActivityParty
                                {
                                    PartyId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item2["idparty"].ToString())
                                };
                                actividad.From = new ActivityParty[] { From };
                            }
                            if (item2["tipo"].ToString() == "2" && item2["tipoparty"].ToString() == "8")
                            {
                                ActivityParty To = new ActivityParty
                                {
                                    PartyId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item2["idparty"].ToString())
                                };
                                actividad.To = new ActivityParty[] { To };
                            }
                            if (item2["tipo"].ToString() == "3" && item2["tipoparty"].ToString() == "8")
                            {
                                ActivityParty CC = new ActivityParty
                                {
                                    PartyId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item2["idparty"].ToString())
                                };
                                actividad.Cc = new ActivityParty[] { CC };
                            }
                            if (item2["tipo"].ToString() == "4" && item2["tipoparty"].ToString() == "8")
                            {
                                ActivityParty BCC = new ActivityParty
                                {
                                    PartyId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item2["idparty"].ToString())
                                };
                                actividad.Bcc = new ActivityParty[] { BCC };
                            }

                        }

                        try
                        {
                            UpsertRequest request = new UpsertRequest()
                            {
                                Target = actividad
                            };
                            UpsertResponse response = (UpsertResponse)CRM.Execute(request);
                            #region actualizar despues de crear
                            if (response.RecordCreated)
                            {
                                Console.WriteLine("Actividad  Creada... revisando estatus ");
                                //Console.ReadKey();
                                // search for an update of status and other attributes
                                actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                                int estado = Convert.ToInt32(item["StateCode"].ToString());
                                switch (estado)
                                {
                                    case 0:
                                        actividad.StateCode = EmailState.Open; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 1:
                                        actividad.StateCode = EmailState.Completed; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 2:
                                        actividad.StateCode = EmailState.Canceled; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                }
                                UpsertRequest request2 = new UpsertRequest()
                                {
                                    Target = actividad
                                };
                                UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);
                                Console.WriteLine("Actividad Actualizada!");
                            }
                            else
                            {
                                actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                                int estado = Convert.ToInt32(item["StateCode"].ToString());
                                switch (estado)
                                {
                                    case 0:
                                        actividad.StateCode = EmailState.Open; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 1:
                                        actividad.StateCode = EmailState.Completed; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                    case 2:
                                        actividad.StateCode = EmailState.Canceled; actividad.StatusCode = new OptionSetValue(Convert.ToInt32(item["StatusCode"].ToString()));
                                        break;
                                }
                                UpsertRequest request2 = new UpsertRequest()
                                {
                                    Target = actividad
                                };
                                UpsertResponse response2 = (UpsertResponse)CRM.Execute(request);
                                Console.WriteLine("Actividad Actualizada!");
                                //Console.ReadKey();
                            }


                        }
                        #endregion
                        catch (Exception e)
                        {
                            Errores++;
                            SendErrorToText(e, "Error No: " + Errores + " . Registro: "+Actual+" "+ item["Subject"].ToString(), item["activityid"].ToString(), "Email");
                            Console.WriteLine("Error !");
                            Console.ReadKey();
                            continue;
                           

                        }
                    }
                    break;
                #endregion
                case 4201:
                    string appointment = "";
                    break;
            }

        }
        private static String asignarparties(String IdActividad)
        {
            String Query = "Select partyuser.InternalEMailAddress as CallFromParty from ActivityParty as parties inner join SystemUser partyuser on  parties.PartyId=partyuser.SystemUserId where parties.ActivityId = '" + IdActividad + "' and ParticipationTypeMask =1";
            DataTable datos = EjecutaQuery(Query);
            String resultado = "";
            foreach (DataRow item in datos.Rows)
            {
                resultado = item["CallFromParty"].ToString();
            }
            return resultado;
        }
        private static void enviarNotas(OrganizationService CRM)
        {
            //String Query = "EXEC SELECT_ANNOTATIONS";
            String Query = "select nota.*,propietario.InternalEMailAddress, propietario.FullName as propietario from Annotation nota inner join SystemUser as propietario on nota.ownerid = propietario.SystemUserId where nota.annotationid='a09a5221-e665-e011-9de1-001e0bfcba2b'";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            int Errores = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Annotation Nota = new Annotation();
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                Console.WriteLine("Registo: " + Actual + " de " + Total + " Nota de " + item["Subject"].ToString() + " de " + item["propietario"].ToString());
                if (existepropietario) { Nota.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { Nota.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                Nota.ObjectId = new EntityReference(Account.EntityLogicalName, "accountnumber", item["ObjectId"].ToString());
                if (item["CreatedOn"].ToString() != "") { DateTime creadael = DateTime.Parse(item["ModifiedOn"].ToString()); Nota.OverriddenCreatedOn = DateTime.Parse(item["CreatedOn"].ToString()); Nota.Subject = item["Subject"].ToString() + " Nota creada el " + creadael.AddHours(-5) + " por " + item["ModifiedByName"].ToString(); } else { Nota.Subject = item["Subject"].ToString() + " Nota creada por " + item["OwnerIdName"].ToString(); }
                Nota.NoteText = item["NoteText"].ToString();
                if (item["IsDocument"].ToString() != "False")
                {
                    Nota.IsDocument = true;
                    Nota.MimeType = item["MimeType"].ToString();
                    Nota.DocumentBody = item["DocumentBody"].ToString();
                    Nota.FileName = item["FileName"].ToString();
                }
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = Nota
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Nota  " + item["Subject"].ToString() + " enviado con exito ");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . ID Nota " + item["AnnotationId"].ToString(), item["Subject"].ToString(), "Notas");
                    continue;

                }

            }
        }
        private static void enviarCuentas()
        {
            OrganizationService CRM = ConexionCRM();
            String Query = "EXEC dbo.Select_cuentas";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            int Errores = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Account Cuenta = new Account();
                if (Convert.ToInt32(item["StateCode"].ToString()) == 1) { Cuenta.StateCode = AccountState.Inactive; Cuenta.StatusCode = new OptionSetValue(2); }
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                if (item["LastUsedInCampaign"].ToString() != "") { Cuenta.LastUsedInCampaign = DateTime.Parse(item["LastUsedInCampaign"].ToString()); }
                Console.WriteLine("Informacion del cliente " + item["Name"].ToString() + " Registo:" + Actual + " de " + Total);
                Cuenta.KeyAttributes = new KeyAttributeCollection { { "accountnumber", item["AccountId"].ToString() } };
                Cuenta.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                if (existepropietario) { Cuenta.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { Cuenta.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                DateTime fechadecreacion = DateTime.Parse(item["createdon"].ToString());
                Cuenta.OverriddenCreatedOn = fechadecreacion.AddHours(-5);
                if (item["Description"].ToString() != "") { Cuenta.Description = item["Description"].ToString(); } else { Cuenta.Description = ""; }
                Cuenta.Name = item["Name"].ToString();
                Cuenta.YomiName = item["YomiName"].ToString();
                Cuenta.new_RFC = item["New_RFC"].ToString();
                Cuenta.WebSiteURL = item["WebSiteURL"].ToString();
                Cuenta.ParentAccountId = (item["ParentAccountId"].ToString() != "") ? (new EntityReference(Account.EntityLogicalName, "accountnumber", item["ParentAccountId"].ToString())) : null;
                Cuenta.New_RelacionesdeNegocioId = (item["new_relacionesdenegocioidname"].ToString() != "") ? (new EntityReference(New_relacionesdenegocio.EntityLogicalName, "new_name", item["new_relacionesdenegocioidname"].ToString())) : null;
                Cuenta.WebSiteURL = item["WebSiteURL"].ToString();
                Cuenta.Telephone1 = item["Telephone1"].ToString();
                Cuenta.Telephone2 = item["Telephone2"].ToString();
                Cuenta.Telephone3 = item["Telephone3"].ToString();
                Cuenta.new_Telefono4 = item["New_Telefono4"].ToString();
                Cuenta.Fax = item["Fax"].ToString();
                Cuenta.EMailAddress1 = item["EMailAddress1"].ToString();
                Cuenta.EMailAddress2 = item["EMailAddress2"].ToString();
                Cuenta.Address1_Line1 = item["address1_line1"].ToString();
                Cuenta.new_Colonia = item["New_Colonia"].ToString();
                Cuenta.Address1_City = item["address1_city"].ToString();
                Cuenta.Address1_PostalCode = item["address1_postalcode"].ToString();
                Cuenta.New_EstadoId = (item["new_estadoidname"].ToString() != "") ? (new EntityReference(New_estado.EntityLogicalName, "new_descripcion", item["new_estadoidname"].ToString())) : null;
                Cuenta.New_PasId = (item["new_pasidname"].ToString() != "") ? (new EntityReference(New_pais.EntityLogicalName, "new_descripcion", item["new_pasidname"].ToString())) : null;
                Cuenta.CustomerSizeCode = (item["New_Tamaodeempresa"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["New_Tamaodeempresa"]))) : null;
                if (item["New_Number"].ToString() != "") { Cuenta.new_Numerodeempleados = item["New_Number"].ToString(); }
                Cuenta.new_NmerodecomputadorasERP = (item["New_Nmerodecompuatadoras"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["New_Nmerodecompuatadoras"]))) : null;
                Cuenta.new_NmerodesucursaleslocalizacionesERP = (item["New_Numerodesucursaleslocalizaciones"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["New_Numerodesucursaleslocalizaciones"]))) : null;
                if (item["New_SectorEconomico"].ToString() != "") { Cuenta.IndustryCode = new OptionSetValue(Convert.ToInt32(item["New_SectorEconomico"])); }
                Cuenta.new_CdigoSICId = (item["new_cdigosicidname"].ToString() != "") ? (new EntityReference(New_cdigosic.EntityLogicalName, "new_codigosic", item["new_cdigosicidname"].ToString())) : null;
                Cuenta.New_SubCdigoSICId = (item["new_subcdigosicidname"].ToString() != "") ? (new EntityReference(New_subsic.EntityLogicalName, "new_name", item["new_subcdigosicidname"].ToString())) : null;
                Cuenta.New_SoftwareERPId = (item["New_SoftwareERPId"].ToString() != "") ? (new EntityReference(New_softwareerp.EntityLogicalName, "new_iddeswerp", item["New_SoftwareERPId"].ToString())) : null;
                Cuenta.New_FabricantedeSoftwareERPId = (item["new_fabricantedesoftwareerpid"].ToString() != "") ? (new EntityReference(New_fabricantedesoftware.EntityLogicalName, "new_descripcion", item["new_fabricantedesoftwareerpidname"].ToString())) : null;
                Cuenta.new_Distribuidorerpid = (item["new_distribuidorerpid"].ToString() != "") ? (new EntityReference(Competitor.EntityLogicalName, "name", item["new_distribuidorerpidname"].ToString())) : null;
                if (item["New_Fechadeultimaactualizacin"].ToString() != "") { Cuenta.new_FechaultimadeactualizacindelERP = DateTime.Parse(item["New_Fechadeultimaactualizacin"].ToString()); }
                if (item["New_FechaprevistadecambiodeERP"].ToString() != "") { Cuenta.new_FechaprevistadecambiodeERP = DateTime.Parse(item["New_FechaprevistadecambiodeERP"].ToString()); }
                Cuenta.new_Tipodeinteresenlasolucin = (item["New_Tipodeinteresenlasolucion"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["New_Tipodeinteresenlasolucion"]))) : null;
                Cuenta.new_AodeadquicisionERP = item["New_AnodeadquicisionERP"].ToString();
                Cuenta.New_SoftwareCRMId = (item["New_SoftwareCRMId"].ToString() != "") ? (new EntityReference(New_softwarecrm.EntityLogicalName, "new_descripcion", item["new_softwarecrmidname"].ToString())) : null;
                Cuenta.New_FabricantedeSoftwareCRMId = (item["New_FabricantedeSoftwareCRMId"].ToString() != "") ? (new EntityReference(New_fabricantedesoftware.EntityLogicalName, "new_descripcion", item["new_fabricantedesoftwarecrmidname"].ToString())) : null;
                Cuenta.new_Distribuidorcrmid = (item["New_DistribuidorcrmId"].ToString() != "") ? (new EntityReference(Competitor.EntityLogicalName, "name", item["new_distribuidorcrmidname"].ToString())) : null;

                if (item["New_NoestasatisfechoconlasolucionCRM"].ToString() != "") { Cuenta.new_NoestsatisfechoconlasolucinCRM = bool.Parse(item["New_NoestasatisfechoconlasolucionCRM"].ToString()); } else { Cuenta.new_NoestsatisfechoconlasolucinCRM = null; }
                if (item["New_ProcesodecambioERP"].ToString() != "") { Cuenta.new_ProcesodecambioERP = bool.Parse(item["New_ProcesodecambioERP"].ToString()); } else { Cuenta.new_ProcesodecambioERP = null; }
                if (item["New_NoestasatisfechoconlasolucionERP"].ToString() != "") { Cuenta.new_NoestsatisfechoconlasolucinERP = bool.Parse(item["New_NoestasatisfechoconlasolucionERP"].ToString()); } else { Cuenta.new_NoestsatisfechoconlasolucinERP = null; }
                if (item["New_Departamentoinformatico"].ToString() != "") { Cuenta.new_DepartamentoInformtico = bool.Parse(item["New_Departamentoinformatico"].ToString()); } else { Cuenta.new_DepartamentoInformtico = null; }
                if (item["New_Cuentaconservidores"].ToString() != "") { Cuenta.new_CuentaconservidoresERP = bool.Parse(item["New_Cuentaconservidores"].ToString()); } else { Cuenta.new_CuentaconservidoresERP = null; }
                if (item["new_fechaultimadeactualizacincrm"].ToString() != "") { Cuenta.new_FechaultimadeactualizacindelCRM = DateTime.Parse(item["new_fechaultimadeactualizacincrm"].ToString()); }
                Cuenta.new_AodeadquicisionCRM = item["new_anodeadquicisioncrm"].ToString();
                Cuenta.new_NecesidadCRM = item["new_necesidadcrm"].ToString();
                Cuenta.new_Necesidad = item["new_necesidad"].ToString();
                if (item["new_ProcesodecambioCRM"].ToString() != "") { Cuenta.new_ProcesodecambioCRM = bool.Parse(item["new_ProcesodecambioCRM"].ToString()); } else { Cuenta.new_ProcesodecambioCRM = null; }
                if (item["new_FechaprevistadecambiodeCRM"].ToString() != "") { Cuenta.new_FechaprevistadecambiodeCRM = DateTime.Parse(item["new_FechaprevistadecambiodeCRM"].ToString()); }
                if (item["new_Existepresupuestoasignadoparaesteproyecto"].ToString() != "") { Cuenta.new_Existepresupuestoasignadoparaesteproyecto = new OptionSetValue(Convert.ToInt32(item["new_Existepresupuestoasignadoparaesteproyecto"])); }
                if (item["new_existepresupuestoasignadoparaesteprocrm"].ToString() != "") { Cuenta.new_existepresupuestoasignadoparaesteprocrm = new OptionSetValue(Convert.ToInt32(item["new_existepresupuestoasignadoparaesteprocrm"])); }


                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = Cuenta
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Cliente" + item["Name"].ToString() + " actualizado con exito ");
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["Name"].ToString(), item["FullName"].ToString(), "Cuentas");
                    continue;

                }

            }
            Console.WriteLine();
            Console.WriteLine("Proceso de Cuentas finalizado, total procesados: " + Total + ", Correctos: " + (Total - Errores) + ", Errores: " + Errores);
            Console.ReadKey();
        }
        private static void asignarContactoPrimario()
        {
            OrganizationService CRM = ConexionCRM();
            String Query = "select cuenta.*,propietario.InternalEMailAddress, propietario.FullName from Account cuenta inner join SystemUser as propietario on cuenta.ownerid = propietario.SystemUserId where cuenta.PrimaryContactId is not null order by createdon desc";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            int Errores = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Account Cuenta = new Account();
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                Console.WriteLine("Informacion del cliente " + item["Name"].ToString() + " Registo:" + Actual + " de " + Total);
                Cuenta.KeyAttributes = new KeyAttributeCollection { { "accountnumber", item["AccountId"].ToString() } };
                Cuenta.PrimaryContactId = (item["primarycontactid"].ToString() != "") ? (new EntityReference(Contact.EntityLogicalName, "pager", item["primarycontactid"].ToString())) : null;

                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = Cuenta
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Cliente" + item["Name"].ToString() + " actualizado con exito ");
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["Name"].ToString(), item["FullName"].ToString(), "ContactoPrimario");
                    continue;

                }

            }
        }
        private static void enviarContactos()
        {
            OrganizationService CRM = ConexionCRM();
            String Query = "select contacto.*,propietario.InternalEMailAddress from Contact contacto inner join SystemUser propietario on contacto.ownerid = propietario.SystemUserId order by createdon desc";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            int Errores = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {

                Actual++;
                Contact contacto = new Contact();
                if (Convert.ToInt32(item["StateCode"].ToString()) == 1) { contacto.StateCode = ContactState.Inactive; contacto.StatusCode = new OptionSetValue(2); }
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                if (item["LastUsedInCampaign"].ToString() != "") { contacto.LastUsedInCampaign = DateTime.Parse(item["LastUsedInCampaign"].ToString()); }
                Console.WriteLine("Registro: " + Actual + " de " + Total + " :  contacto " + item["FullName"].ToString());
                contacto.KeyAttributes = new KeyAttributeCollection { { "pager", item["contactid"].ToString() } };
                if (existepropietario) { contacto.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { contacto.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                contacto.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                DateTime fechadecreacion = DateTime.Parse(item["createdon"].ToString());
                contacto.OverriddenCreatedOn = fechadecreacion.AddHours(-5);
                contacto.ParentCustomerId = (item["parentcustomerid"].ToString() != "") ? (new EntityReference(Account.EntityLogicalName, "accountnumber", item["parentcustomerid"].ToString())) : null;
                contacto.new_Saludo = (item["New_Saludo"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["New_Saludo"]))) : null;
                contacto.new_Titulo = (item["new_titulo"].ToString() != "") ? (new EntityReference(new_titulo.EntityLogicalName, "new_claveintegracion", item["new_titulo"].ToString())) : null;
                contacto.FirstName = (item["FirstName"].ToString());
                contacto.LastName = (item["LastName"].ToString());
                contacto.MiddleName = (item["New_SegundoApellido"].ToString());
                contacto.New_Departamento_new = (item["New_Departamento_new"].ToString() != "") ? (new EntityReference(New_departamento.EntityLogicalName, "new_clavedeintegracion", item["New_Departamento_new"].ToString())) : null;
                contacto.JobTitle = (item["JobTitle"].ToString());
                contacto.AccountRoleCode = (item["AccountRoleCode"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["AccountRoleCode"]))) : null;
                contacto.EMailAddress1 = (item["EMailAddress1"].ToString());
                contacto.EMailAddress2 = (item["New_Correoelectrnico2"].ToString());
                contacto.Telephone1 = (item["Telephone1"].ToString());
                contacto.Telephone2 = (item["Telephone2"].ToString());
                contacto.Telephone3 = (item["Telephone3"].ToString());
                contacto.new_telefono4 = (item["New_Telefono4"].ToString());
                contacto.new_numerodeextension = (item["New_Numerodeextension"].ToString());
                contacto.MobilePhone = (item["MobilePhone"].ToString());
                if (item["BirthDate"].ToString() != "") { contacto.BirthDate = DateTime.Parse(item["BirthDate"].ToString()); }
                contacto.LeadSourceCode = (item["LeadSourceCode"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["LeadSourceCode"]))) : null;
                contacto.Address1_Line1 = item["address1_line1"].ToString();
                contacto.new_Colonia = item["New_Colonia"].ToString();
                contacto.Address1_City = item["address1_city"].ToString();
                contacto.Address1_PostalCode = item["address1_postalcode"].ToString();
                contacto.New_EstadoId = (item["new_estadoidname"].ToString() != "") ? (new EntityReference(New_estado.EntityLogicalName, "new_descripcion", item["new_estadoidname"].ToString())) : null;
                contacto.New_PasId = (item["new_pasidname"].ToString() != "") ? (new EntityReference(New_pais.EntityLogicalName, "new_descripcion", item["new_pasidname"].ToString())) : null;
                contacto.new_Interesadoen = (item["new_Interesadoen"].ToString() != "") ? (new OptionSetValue(Convert.ToInt32(item["new_Interesadoen"]))) : null;
                contacto.Description = (item["Description"].ToString());

                if (item["new_Tienecorreo"].ToString() != "") { contacto.new_tienecorreo = bool.Parse(item["new_Tienecorreo"].ToString()); } else { contacto.new_tienecorreo = null; }
                if (item["DoNotSendMM"].ToString() != "") { contacto.DoNotSendMM = bool.Parse(item["DoNotSendMM"].ToString()); } else { contacto.DoNotSendMM = null; }
                if (item["New_SuscritoaNewsletter"].ToString() != "") { contacto.new_SuscritoaNewsletter = bool.Parse(item["New_SuscritoaNewsletter"].ToString()); } else { contacto.new_SuscritoaNewsletter = null; }
                if (item["new_NoenvarinformacindeOffice"].ToString() != "") { contacto.new_NoenvarinformacindeOffice = bool.Parse(item["new_NoenvarinformacindeOffice"].ToString()); } else { contacto.new_NoenvarinformacindeOffice = null; }
                if (item["New_NoenvarmaterialesdeMarketingdeAzulAcuatic"].ToString() != "") { contacto.new_NoEnvarmaterialesdeMarketingdeAzulAcuatic = bool.Parse(item["New_NoenvarmaterialesdeMarketingdeAzulAcuatic"].ToString()); } else { contacto.new_NoEnvarmaterialesdeMarketingdeAzulAcuatic = null; }
                if (item["new_NoenvarinformacindeConjuntoSantaAnita"].ToString() != "") { contacto.new_NoenvarinformacindeConjuntoSantaAnita = bool.Parse(item["new_NoenvarinformacindeConjuntoSantaAnita"].ToString()); } else { contacto.new_NoenvarinformacindeConjuntoSantaAnita = null; }
                contacto.cdi_linkedin = item["cdi_linkedin"].ToString();
                contacto.cdi_twitter = item["cdi_twitter"].ToString();
                contacto.cdi_facebook = item["cdi_facebook"].ToString();
                contacto.cdi_image = item["cdi_image"].ToString();
                if (item["cdi_identifiedon"].ToString() != "") { contacto.cdi_identifiedon = DateTime.Parse(item[""].ToString()).AddHours(-5); }
                if (item["cdi_allowtextmessages"].ToString() != "") { contacto.cdi_allowtextmessages = bool.Parse(item["cdi_allowtextmessages"].ToString()); }
                if (item["cdi_age"].ToString() != "") { contacto.cdi_age = int.Parse(item["cdi_age"].ToString()); }


                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = contacto
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Contacto  " + item["FullName"].ToString() + " actualizado con exito ");

                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["Fullname"].ToString(), item["InternalEMailAddress"].ToString(), "Contactos");
                    continue;
                }

            }
        }
        #region funcion complemento
        private static void getRelacionNegocio(OrganizationService CRM)
        {
            String Query = "SELECT * FROM new_relacionesdenegocio";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_relacionesdenegocio relacion = new New_relacionesdenegocio();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["new_name"].ToString());
                relacion.KeyAttributes = new KeyAttributeCollection { { "new_name", item["new_name"].ToString() } };
                relacion.New_Descripcion = item["new_descripcion"].ToString();

                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = relacion
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["new_name"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["new_name"].ToString(), item["new_name"].ToString(), "RelacionesNegocios");
                    continue;

                }

            }
        }
        private static void getCodigoSic(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_cdigosic";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_cdigosic sic = new New_cdigosic();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["New_codigosic"].ToString());
                sic.KeyAttributes = new KeyAttributeCollection { { "new_codigosic", item["New_codigosic"].ToString() } };
                sic.New_Nombre = item["New_Nombre"].ToString();

                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = sic
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["New_codigosic"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_codigosic"].ToString(), item["New_codigosic"].ToString(), "SIC");
                    continue;

                }

            }
        }
        private static void getCodigoSubsic(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_subsic";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_subsic sic = new New_subsic();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["New_name"].ToString());
                sic.KeyAttributes = new KeyAttributeCollection { { "new_name", item["New_name"].ToString() } };
                sic.New_Descripcin = item["New_Descripcin"].ToString();

                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = sic
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["New_name"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_name"].ToString(), item["New_name"].ToString(), "SubSIC");
                    continue;

                }

            }
        }
        private static void getCompetidores(OrganizationService CRM)
        {
            String Query = "SELECT * FROM Competitor";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Competitor registro = new Competitor();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["name"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "name", item["name"].ToString() } };
                registro.new_Descripcion = item["New_Descripcion"].ToString();
                registro.Strengths = item["Strengths"].ToString();
                registro.Weaknesses = item["Weaknesses"].ToString();
                registro.WebSiteUrl = item["WebSiteUrl"].ToString();
                registro.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["name"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["name"].ToString(), item["name"].ToString(), "Competidor");
                    Console.ReadKey();
                    continue;

                }

            }

        }
        private static void getPaises(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_pais";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_pais registro = new New_pais();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["New_descripcion"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_descripcion", item["New_descripcion"].ToString() } };
                registro.New_Codigo = item["New_Codigo"].ToString();


                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["New_descripcion"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_descripcion"].ToString(), item["New_descripcion"].ToString(), "Pais");
                    Console.ReadKey();
                    continue;

                }

            }

        }
        private static void getEstados(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_estado";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_estado registro = new New_estado();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["New_descripcion"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_descripcion", item["New_descripcion"].ToString() } };
                registro.New_Codigo = item["New_Codigo"].ToString();
                registro.new_Pas = (item["new_PasName"].ToString() != "") ? (new EntityReference(New_pais.EntityLogicalName, "new_descripcion", item["new_PasName"].ToString())) : null;
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["New_descripcion"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_descripcion"].ToString(), item["New_descripcion"].ToString(), "Estado");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        private static void getFabricanteSW(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_fabricantedesoftware";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_fabricantedesoftware registro = new New_fabricantedesoftware();
                Console.WriteLine(" Registro:" + Actual + " de " + Total + " : " + item["New_descripcion"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_descripcion", item["New_descripcion"].ToString() } };
                registro.New_Codigo = item["New_Codigo"].ToString();
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Registro: " + item["New_descripcion"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_descripcion"].ToString(), item["New_descripcion"].ToString(), "FabricanteSW");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        private static void getSWERP(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_softwareerp";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_softwareerp registro = new New_softwareerp();
                Console.WriteLine("Registro:" + Actual + " de " + Total + " : " + item["New_descripcion"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_iddeswerp", item["New_softwareerpId"].ToString() } };
                registro.New_Codigo = item["New_Codigo"].ToString();
                registro.New_descripcion = item["New_descripcion"].ToString();
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine(" " + item["New_descripcion"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_descripcion"].ToString(), item["New_descripcion"].ToString(), "SWERP");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        private static void getSWCRM(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_softwarecrm";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_softwarecrm registro = new New_softwarecrm();
                Console.WriteLine("Registro:" + Actual + " de " + Total + " : " + item["New_descripcion"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_descripcion", item["New_descripcion"].ToString() } };
                registro.New_Codigo = item["New_Codigo"].ToString();
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine(item["New_descripcion"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_descripcion"].ToString(), item["New_descripcion"].ToString(), "SWCRM");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        private static void getTitulosContacto(OrganizationService CRM)
        {
            String Query = "SELECT * FROM new_titulo";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                new_titulo registro = new new_titulo();
                Console.WriteLine("Registro:" + Actual + " de " + Total + " : " + item["new_name"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_claveintegracion", item["new_tituloid"].ToString() } };
                registro.new_Titulo = item["new_titulo"].ToString();
                registro.new_name = item["new_name"].ToString();
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine(item["new_name"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["new_name"].ToString(), item["new_name"].ToString(), "TitulosContacto");
                    Console.WriteLine("Error! ");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        private static void getDepartamentosContacto(OrganizationService CRM)
        {
            String Query = "SELECT * FROM New_departamento";
            int Total = 0;
            DataTable datos = EjecutaQuery(Query);
            int Actual = 0;
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                New_departamento registro = new New_departamento();
                Console.WriteLine("Registro:" + Actual + " de " + Total + " : " + item["New_departamento"].ToString());
                registro.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["New_departamentoId"].ToString() } };
                registro.New_departamento1 = item["New_departamento"].ToString();
                if (item["New_id"].ToString() != "") registro.New_id = Convert.ToInt32(item["New_id"].ToString());

                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = registro
                    };
                    CRM.Execute(request);
                    Console.WriteLine(item["New_departamento"].ToString() + " actualizado");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    SendErrorToText(e, item["New_departamento"].ToString(), item["New_departamento"].ToString(), "DeptosContacto");
                    Console.ReadKey();
                    continue;
                }

            }

        }
        #endregion
        #region funciones generales y de conexion
        private static OrganizationService ConexionCRM()
        {
            //String TENANT = "atx";
            String USERNAME = "crmadmin@atx.mx";
            String PASSWORD = "Bora2091";
            string connectionString = "ServiceUri=https://atx.api.crm.dynamics.com/XRMServices/2011/Organization.svc; UserName=" + USERNAME + "; Password=" + PASSWORD;
            CrmConnection connection = CrmConnection.Parse(connectionString);
            OrganizationService servicio = new OrganizationService(connection);
            return servicio;
        }
        private static DataTable EjecutaQuery(string queryString)
        {
            string connectionString = "Server=srvdynamics;Database=AutomatizacionesIndustrialesydeOficinaSAdeCV; Trusted_Connection=True;";
            var ds = new DataSet();
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var command = new SqlCommand(queryString, conn);
                var adapter = new SqlDataAdapter(command);
                adapter.Fill(ds, "Data");
                conn.Close();
            }
            DataTable dt = ds.Tables["Data"];
            return dt;
        }
        private static Boolean VerificaPropietario(string correo)
        {
            OrganizationService CRM = ConexionCRM();
            ConditionExpression cond2 = new ConditionExpression();
            cond2.AttributeName = "internalemailaddress";
            cond2.Operator = ConditionOperator.Equal;
            cond2.Values.Add(correo);

            QueryExpression consulta = new QueryExpression(SystemUser.EntityLogicalName);
            consulta.PageInfo.ReturnTotalRecordCount = true;
            consulta.ColumnSet = new ColumnSet(new String[] { "fullname" });
            consulta.Criteria.AddCondition(cond2);
            EntityCollection resultado = new EntityCollection();
            resultado = CRM.RetrieveMultiple(consulta);
            if (resultado.TotalRecordCount != 0) { return true; } else { return false; }
        }
        public static void SendErrorToText(Exception ex, String cliente, string propietario, string filename)
        {
            var line = Environment.NewLine;
            ErrorlineNo = ex.StackTrace.Substring(ex.StackTrace.Length - 7, 7);
            Errormsg = ex.GetType().Name.ToString();
            extype = ex.GetType().ToString();
            ErrorLocation = ex.Message.ToString();
            StreamWriter log;
            string filePath = filename + "_Log.txt";
            if (!File.Exists(filePath))
            {
                log = new StreamWriter(filePath);
            }
            else
            {
                log = new StreamWriter(filePath, true);
            }

            // Write to the file:
            log.WriteLine(DateTime.Now.ToString() + "Registro: " + cliente + " de " + propietario);
            log.WriteLine("Error line:     " + ErrorlineNo);
            log.WriteLine("Error type:     " + extype);
            log.WriteLine("Error location: " + ErrorLocation);
            log.WriteLine("--------------------------------*End*------------------------------------------");
            log.WriteLine();
            // Close the stream:
            log.Flush();
            log.Close();
        }
        private static void DeleteRecords(OrganizationService CRM)
        {
            Console.WriteLine("Eliminando Informacion ...");

            BulkDeleteRequest request = new BulkDeleteRequest
            {
                JobName = "Depuracion de registros...",
                ToRecipients = new Guid[] { },
                CCRecipients = new Guid[] { },
                RecurrencePattern = string.Empty,
                QuerySet = new QueryExpression[]
                {
                    new QueryExpression
                    {
                        EntityName = PhoneCall.EntityLogicalName,
                        Criteria =
                        {
                            Filters =
                            {
                                new FilterExpression
                                {
                                    FilterOperator = LogicalOperator.Or,
                                    Conditions =
                                    {
                                        new ConditionExpression("modifiedon", ConditionOperator.LastMonth),
                                        new ConditionExpression("modifiedon", ConditionOperator.ThisMonth)
                                    },
                                },
                            },

                    }
                }
            }
            };

            BulkDeleteResponse response = (BulkDeleteResponse)CRM.Execute(request);
            Guid jobId = response.JobId;

            bool deleting = true;

            while (deleting)
            {
                Console.WriteLine("Borrando...");
                Thread.Sleep(10000);    // poll crm every 10 seconds 

                QueryExpression query = new QueryExpression { EntityName = "bulkdeleteoperation" };
                query.Criteria.AddCondition("asyncoperationid", ConditionOperator.Equal, jobId);
                query.Criteria.AddCondition("statecode", ConditionOperator.Equal, 3);
                query.Criteria.AddCondition("statuscode", ConditionOperator.Equal, 30);

                EntityCollection results = CRM.RetrieveMultiple(query);
                if (results.Entities.Count > 0)
                {
                    Console.WriteLine("Informacion Eliminada!!!");
                    deleting = false;
                }
            }
        }
        #endregion
        #endregion
    }
}
