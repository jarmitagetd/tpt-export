/*------------------------------------------------------------------------*/
/* README:    TPT export for large transactional volume and reference data*/
/* AUTHOR:    James Armitage, Teradata                                    */
/* VERSION:   1.0                                                         */
/* CHANGELOG:                                                             */
/* GIT:       https://github.com/jarmitagetd/tpt-export       
/*------------------------------------------------------------------------*/        
/* LICENSE: TPT export for large transactional volume and reference data. */
/*          Copyright (C) 2017 James Armitage                             */
/*                                                                        */
/*  This program is free software: you can redistribute it and/or modify  */
/*  it under the terms of the GNU General Public License as published by  */
/*  the Free Software Foundation, either version 3 of the License, or     */
/*  (at your option) any later version.                                   */
/*                                                                        */
/*  This program is distributed in the hope that it will be useful,       */
/*  but WITHOUT ANY WARRANTY; without even the implied warranty of        */
/*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         */
/*  GNU General Public License for more details.                          */
/*                                                                        */
/*  You should have received a copy of the GNU General Public License     */
/*  along with this program.  If not, see <http://www.gnu.org/licenses/>. */
/*------------------------------------------------------------------------*/                                                       
/* INSTALL:   1) Copy or git clone the folder\repo tptexport to your      */ 
/*               Linux home directory.                                    */
/*            2) Excute permissions.sh as root\sudo (steps below)         */
/*                -- sudo groupadd tpt-export                             */
/*                -- sudo useradd -g tpt-export tpt-export                */
/*                -- sudo chown -R tpt-export:tpt-export tpt-export/      */
/*                -- sudo chmod -R 0755 tpt-export                        */
/*            3) Set your Teradata logon credentials and TPT operator     */
/*               attributes, output dir path in the file vtpt.            */
/*            4) Set your database, object names, date parameters in      */
/*	             objects.txt. Objects with no date parameters are not not */
/*		         filtered - see template                                  */
/*	          7) Add your SQL to the dir SQL. Create your filename with   */
/*               the following syntax and extension - [objectname].sql    */
/*               The SQL file allows you to tokenise the databasename and */
/*               date parameter - see template                            */
/*------------------------------------------------------------------------*/
