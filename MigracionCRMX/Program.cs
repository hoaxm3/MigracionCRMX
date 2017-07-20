using System;
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

            //enviarCuentas();
            //enviarContactos();
            //asignarContactoPrimario();
            //enviarNotas(CRM);
            enviarActividades(CRM, "32D42758-E52F-E011-862B-001E0BFCBA2B");
            //enviarlistasdemkt(CRM);


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
            #endregion
        }

        #region Funciones 
      /*  private static void enviarlistasdemkt( OrganizationService CRM)
        {
            int Total = 0, Actual = 0, Errores = 0;
            String Query = "select  * from list";
            DataTable datos = EjecutaQuery(Query);
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Console.WriteLine("Registo: " + Actual + " de " + Total + " Lista: " + item["ListName"].ToString() + " de " + item["OwnerIdName"].ToString());
                Console.ReadKey();
                List lista = new List();
                lista.KeyAttributes = new KeyAttributeCollection { { "new_clavedelista", item["ListId"].ToString() } };
                lista.ListName = item["ListName"].ToString();
                lista.Type = bool.Parse(item["Type"].ToString());
                lista.MemberType = int.Parse(item["MemberType"].ToString());
                lista.Purpose = item["Purpose"].ToString();
                lista.OwnerId =;
                lista.OverriddenCreatedOn = DateTime.Parse(item["CreatedOn"].ToString());
                lista.Cost = new Money(Decimal.Parse(item["Cost"].ToString()));
                lista.Description = item["Description"].ToString();
                lista.TransactionCurrencyId = (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null; ;
                lista.StateCode =;
                lista.DoNotSendOnOptOut =bool.Parse(item["DoNotSendOnOptOut"].ToString());
                //lista.new_campania = ;
                lista.IgnoreInactiveListMembers = bool.Parse(item["IgnoreInactiveListMembers"].ToString());
                lista.Source = item["Source"].ToString();
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = lista
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Lista " + item["ListName"].ToString() + " actualizado con exito ");
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["ListName"].ToString(), item["FullName"].ToString(), "Listas_MKT");
                    continue;

                }


            }

        }
        */
        private static void enviarActividades(OrganizationService CRM, string objectidentificador)
        {
            int Total = 0, Actual = 0, Errores = 0;
            String Query = "select  * from FilteredActivityPointer as actividad inner join SystemUser as propietario on actividad.ownerid = propietario.SystemUserId where actividad.regardingobjecttypecode=1 and actividad.regardingobjectid = '" + objectidentificador+"' order by actividad.modifiedOn desc";
            DataTable datos = EjecutaQuery(Query);
            Total = datos.Rows.Count;
            foreach (DataRow item in datos.Rows)
            {
                Actual++;
                Console.WriteLine("Registro: " + Actual + " de " + Total + " Actividad: " + item["activitytypecodename"].ToString() + " a " + item["RegardingObjectIdName"].ToString() + item["OwnerIdName"].ToString() + item["Subject"].ToString());
                PhoneCall actividad = new PhoneCall();
                PhoneCall llamada = new PhoneCall();
                bool existepropietario = VerificaPropietario(item["InternalEMailAddress"].ToString());
                if (existepropietario) { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { actividad.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                actividad.KeyAttributes = new KeyAttributeCollection { { "new_clavedeintegracion", item["ActivityId"].ToString() } };
                actividad.Subject = item["Subject"].ToString();
                actividad.ActualDurationMinutes = Convert.ToInt32(item["ActualDurationMinutes"].ToString());
                actividad.ActualEnd = DateTime.Parse(item["ActualEnd"].ToString());
                //actividad.ActualStart = DateTime.Parse(item["ActualStart"].ToString());
                actividad.TransactionCurrencyId= (item["TransactionCurrencyIdName"].ToString() != "") ? (new EntityReference(TransactionCurrency.EntityLogicalName, "currencyname", item["TransactionCurrencyIdName"].ToString())) : null;
                actividad.Description = item["Description"].ToString();
                DateTime fecha_creacion = DateTime.Parse(item["CreatedOn"].ToString());
                actividad.OverriddenCreatedOn =fecha_creacion.AddHours(-5);
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
                try
                {
                    UpsertRequest request = new UpsertRequest()
                    {
                        Target = actividad
                    };
                    CRM.Execute(request);
                    Console.WriteLine("Actividad " + item["activitytypecodename"].ToString() + " actualizado con exito ");
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . " + item["activitytypecodename"].ToString(), item["activityid"].ToString(), "Llamadas");
                    continue;

                }

            }
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
                Console.WriteLine("Registo: " + Actual + " de " + Total+" Nota de "+ item["Subject"].ToString()+" de "+item["propietario"].ToString());
                if (existepropietario) { Nota.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", item["InternalEMailAddress"].ToString()); } else { Nota.OwnerId = new EntityReference(SystemUser.EntityLogicalName, "internalemailaddress", "mlsosa@atx.mx"); }
                Nota.ObjectId= new EntityReference(Account.EntityLogicalName, "accountnumber", item["ObjectId"].ToString());
                if (item["CreatedOn"].ToString()!="") { DateTime creadael = DateTime.Parse(item["ModifiedOn"].ToString()); Nota.OverriddenCreatedOn =DateTime.Parse(item["CreatedOn"].ToString()); Nota.Subject = item["Subject"].ToString() + " Nota creada el " + creadael.AddHours(-5) + " por " + item["ModifiedByName"].ToString(); } else { Nota.Subject = item["Subject"].ToString() + " Nota creada por " + item["OwnerIdName"].ToString(); }
                Nota.NoteText = item["NoteText"].ToString();
                if (item["IsDocument"].ToString()!="False")
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
                    Console.WriteLine("Nota  " +item["Subject"].ToString() + " enviado con exito ");
                    Console.ReadKey();
                }
                catch (Exception e)
                {
                    Errores++;
                    SendErrorToText(e, "Error: " + Errores + " . ID Nota " +item["AnnotationId"].ToString(), item["Subject"].ToString(), "Notas");
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
                Cuenta.AccountNumber = item["AccountId"].ToString();
                if (item["Description"].ToString() != "") { Cuenta.Description = item["Description"].ToString(); } else { Cuenta.Description = ""; }
                Cuenta.Name = item["Name"].ToString();
                Cuenta.YomiName = item["YomiName"].ToString();
                Cuenta.new_RFC = item["New_RFC"].ToString();
                Cuenta.ParentAccountId = (item["ParentAccountId"].ToString() != "") ? (new EntityReference(Account.EntityLogicalName, "accountnumber", item["ParentAccountId"].ToString())) : null;
                Cuenta.New_RelacionesdeNegocioId = (item["new_relacionesdenegocioidname"] != null) ? (new EntityReference(New_relacionesdenegocio.EntityLogicalName, "new_name", item["new_relacionesdenegocioidname"].ToString())) : null;
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

                if (item["New_NoestasatisfechoconlasolucionCRM"].ToString() != "") { Cuenta.new_NoestsatisfechoconlasolucinCRM = bool.Parse(item["New_NoestasatisfechoconlasolucionCRM"].ToString()); }else { Cuenta.new_NoestsatisfechoconlasolucinCRM = null; }
                if (item["New_ProcesodecambioERP"].ToString() != "") { Cuenta.new_ProcesodecambioERP = bool.Parse(item["New_ProcesodecambioERP"].ToString()); } else { Cuenta.new_ProcesodecambioERP = null; }
                if (item["New_NoestasatisfechoconlasolucionERP"].ToString() != "") { Cuenta.new_NoestsatisfechoconlasolucinERP = bool.Parse(item["New_NoestasatisfechoconlasolucionERP"].ToString()); } else { Cuenta.new_NoestsatisfechoconlasolucinERP = null; }
                if (item["New_Departamentoinformatico"].ToString() != "") { Cuenta.new_DepartamentoInformtico = bool.Parse(item["New_Departamentoinformatico"].ToString()); }else { Cuenta.new_DepartamentoInformtico = null; }
                if (item["New_Cuentaconservidores"].ToString() != "") { Cuenta.new_CuentaconservidoresERP = bool.Parse(item["New_Cuentaconservidores"].ToString()); }else { Cuenta.new_CuentaconservidoresERP = null; }
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
                    SendErrorToText(e, "Error: "+Errores+" . "+item["Name"].ToString(), item["FullName"].ToString(),"Cuentas");
                    continue;

                }

            }
            Console.WriteLine();
            Console.WriteLine("Proceso de Cuentas finalizado, total procesados: "+Total+ ", Correctos: " + (Total-Errores)+", Errores: " + Errores );
            Console.ReadKey();
        }
        private static void asignarContactoPrimario()
        {
            OrganizationService CRM = ConexionCRM();
            String Query = "select cuenta.*,propietario.InternalEMailAddress, propietario.FullName from Account cuenta inner join SystemUser as propietario on cuenta.ownerid = propietario.SystemUserId where cuenta.PrimaryContactId is not null";
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
                    SendErrorToText(e, "Error: " + Errores + " . "+item["Name"].ToString(), item["FullName"].ToString(),"ContactoPrimario");
                    continue;

                }

            }
        }
        private static void enviarContactos()
        {
            OrganizationService CRM = ConexionCRM();
            String Query = "select contacto.*,propietario.InternalEMailAddress from Contact contacto inner join SystemUser propietario on contacto.ownerid = propietario.SystemUserId";
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
                    SendErrorToText(e, "Error: " + Errores + " . "+item["Fullname"].ToString(), item["InternalEMailAddress"].ToString(),"Contactos");
                    continue;
                }

            }
        }
        private static void geteRelacionNegocio(OrganizationService CRM)
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
                Console.WriteLine(" Registro:" + Actual + " de " + Total +" : "+ item["new_name"].ToString());
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
                    SendErrorToText(e, item["new_name"].ToString(), item["new_name"].ToString(),"RelacionesNegocios");
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
                registro.Strengths = item["Strengths"].ToString() ;
                registro.Weaknesses= item["Weaknesses"].ToString();
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
                registro.New_departamento1= item["New_departamento"].ToString();
                if(item["New_id"].ToString()!="")  registro.New_id = Convert.ToInt32(item["New_id"].ToString());

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

        private static OrganizationService ConexionCRM()
        {
            //String TENANT = "atx";
            String USERNAME = "equintero@atx.mx";
            String PASSWORD = "/MONSTERzz3";
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
            string filePath = filename+"_Log.txt";
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
        #endregion
    }
}
