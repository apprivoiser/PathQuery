/*=============================================================================
# Filename: gquery.cpp
# Author: Bookug Lobert
# Mail: 1181955272@qq.com
# Last Modified: 2015-10-20 12:23
# Description: query a database, there are several ways to use this program:
1. ./gquery                                        print the help message
2. ./gquery --help                                 simplified as -h, equal to 1
3. ./gquery db_folder query_path                   load query from given path fro given database
4. ./gquery db_folder                              load the given database and open console
=============================================================================*/

#include "../Database/Database.h"
#include "../Util/Util.h"

#include<pthread.h>
#include <sys/time.h>
using namespace std;

//WARN:cannot support soft links!

void
help()
{
	printf("\
			/*=============================================================================\n\
# Filename: gquery.cpp\n\
# Author: Bookug Lobert\n\
# Mail: 1181955272@qq.com\n\
# Last Modified: 2015-10-20 12:23\n\
# Description: query a database, there are several ways to use this program:\n\
1. ./gquery                                        print the help message\n\
2. ./gquery --help                                 simplified as -h, equal to 1\n\
3. ./gquery db_folder query_path                   load query from given path fro given database\n\
4. ./gquery db_folder                              load the given database and open console\n\
=============================================================================*/\n");
}


void Query(string& ssparql,vector<vector<string> >& ans_set,Database& _db)
{
	ResultSet _rs;
	FILE* ofp = stdout;
	// cout<<ssparql<<endl;
	_db.query(ssparql, _rs, ofp);
	string rs=_rs.to_str();
	vector<string> query_ans_tmp=Util::split(rs,"\n");
	// set<string> sub;
	// {
	// 	ssparql="SELECT ?y WHERE {?y <http://purl.org/dc/terms/Location> <http://db.uwaterloo.ca/~galuc/wsdbm/City2> .}";
	// 	_db.query(ssparql, _rs, ofp);
	// 	rs=_rs.to_str();
	// 	vector<string> ssub=Util::split(rs,"\n");
	// 	for(int i=1;i<ssub.size();i++)
	// 		sub.insert(ssub[i]);
	// }

	// for(int i=1;i<query_ans_tmp.size();i++)
	// {
	// 	vector<string> sub_pre_obj=Util::split(query_ans_tmp[i],"\t");
	// 	if(sub.count(sub_pre_obj[0]))
	// 	{
	// 		ans_set.push_back(sub_pre_obj);
	// 	}
	// }
	for(int i=1;i<query_ans_tmp.size();i++)
		ans_set.push_back(Util::split(query_ans_tmp[i],"\t"));
}


tree T[4];
int K;
int St;
graph test;
int dfs(int pos,node* parent,int rt)
{
   if(pos==K-1)return 1;
   int ret=0;
   node* child;
   // cout<<parent->val.first<<" "<<parent->val.second<<endl;
   int obj=test.edge[parent->val.first][parent->val.second].second;
   for(int i=0;i<test.cluster[obj].size();i++)
   {
      child=new node(test.cluster[obj][i]);
      int tmp=dfs(pos+1,child,rt);
      if(tmp)T[rt].addChild(parent,child);
      ret|=tmp;
   }
   return ret|(pos+1>=St);
}

void *Thread(void *arg)
{
   struct timeval tpstart,tpend;
   gettimeofday(&tpstart,NULL);
   int cur=*((int *)arg);
   // cout<<"thread: "<<cur<<endl;
   node* root=new node(make_pair(0,0));
   T[cur].init(root);
   node* child;
   set<int>::iterator it;
   for(int i=0;i<test.edge.size();i++)
   for(int j=cur;j<test.edge[i].size();j++)
   {
      child=new node(make_pair(i,j));
      int tmp=dfs(0,child,cur);
      if(tmp)T[cur].addChild(root,child);
   }
   gettimeofday(&tpend,NULL);
   float timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;       
   timeuse/=1000000;       
   // printf("thread%d used time:%f sec\n",cur,timeuse);
   pthread_exit(NULL);
}


int dfs_hash(int pos,node* parent,int rt)
{
   if(pos+2>K)return pos>=St;
   int ret=0;
   node* child;
   int obj=test.edge[parent->val.first][parent->val.second].second;
   for(int i=0;i<test.cluster[obj].size();i++)
   {
      int next_obj=test.edge[test.cluster[obj][i].first][test.cluster[obj][i].second].second;
      for(int j=0;j<test.cluster[next_obj].size();j++)
      {
         child=new node(test.cluster[next_obj][j]);
         int tmp=dfs_hash(pos+2,child,rt);
         if(tmp)T[rt].addChild(parent,child);
         ret|=tmp;
      }
   }
   return ret|(pos>=St);
}

void *Thread_hash(void *arg)
{
   struct timeval tpstart,tpend;
   gettimeofday(&tpstart,NULL);
   int cur=*((int *)arg);
   // cout<<"thread: "<<cur<<endl;
   node* root=new node(make_pair(0,0));
   T[cur].init(root);
   node* child;
   node* next_child;
   int tag=-1;
   if(St==K)tag=K%2;
   for(int i=0;i<test.edge.size();i++)
   for(int j=0;j<test.edge[i].size();j++)
   {
      child=new node(make_pair(i,j));
      if(!cur&&tag!=0)
      {
        int tmp=dfs_hash(1,child,cur);
        if(tmp)T[cur].addChild(root,child);
      }
      else if(cur&&K>1&&tag!=1)
      {
         int obj=test.edge[i][j].second;
         for(int k=0;k<test.cluster[obj].size();k++)
         {
            next_child=new node(test.cluster[obj][k]);
            int tmp=dfs_hash(2,next_child,cur);  
            if(tmp)T[cur].addChild(child,next_child);
         }
         if(child->children.size()>0)T[cur].addChild(root,child);
      }
   }
   gettimeofday(&tpend,NULL);
   float timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;       
   timeuse/=1000000;       
   printf("thread%d used time:%f sec\n",cur,timeuse);
   pthread_exit(NULL);
}

void _hash(int cur)
{
   struct timeval tpstart,tpend;
   gettimeofday(&tpstart,NULL);
   node* root=new node(make_pair(0,0));
   T[cur].init(root);
   node* child;
   node* next_child;
   int tag=-1;
   if(St==K)tag=K%2;
   for(int i=0;i<test.edge.size();i++)
   for(int j=0;j<test.edge[i].size();j++)
   {
      child=new node(make_pair(i,j));
      if(!cur&&tag!=0)
      {
        int tmp=dfs_hash(1,child,cur);
        if(tmp)T[cur].addChild(root,child);
      }
      else if(cur&&K>1&&tag!=1)
      {
         int obj=test.edge[i][j].second;
         for(int k=0;k<test.cluster[obj].size();k++)
         {
            next_child=new node(test.cluster[obj][k]);
            int tmp=dfs_hash(2,next_child,cur);  
            if(tmp)T[cur].addChild(child,next_child);
         }
         if(child->children.size()>0)T[cur].addChild(root,child);
      }
   }
   gettimeofday(&tpend,NULL);
   float timeuse=1000000*(tpend.tv_sec-tpstart.tv_sec)+tpend.tv_usec-tpstart.tv_usec;       
   timeuse/=1000000;       
   // printf("thread%d used time:%f sec\n",cur,timeuse);
}

void dp(vector<vector<string> >&candidate)
{
	set<int> our_sub;
	set<int> our_obj;
	int V_in=0,V_out=0,V_H=0,E=0;
	int E_in=0,E_out=0,E_H=0;

	V_H=test.subject_set.size()+test.object_set.size()-test.sub_obj_set.size();
	for(int i=0;i<test.edge.size();i++)
	{
		E+=test.edge[i].size();
		for(int j=0;j<test.edge[i].size();j++)
		{
			if(test.sub_obj_set.count(test.edge[i][j].first))
				V_out++;
			if(test.sub_obj_set.count(test.edge[i][j].second))
				V_in++;
			if(test.sub_obj_set.count(test.edge[i][j].first)&&test.sub_obj_set.count(test.edge[i][j].second))
			{
				our_sub.insert(test.edge[i][j].first);
				our_obj.insert(test.edge[i][j].second);
				E_H++;
			}
		}
	}
	// cout<<V_in<<" "<<V_out<<" "<<V_H<<endl;
	double SF_GJ=(double)V_in*V_out/(double)V_H*E*E;

	
	for(int i=0;i<test.edge.size();i++)
		for(int j=0;j<test.edge[i].size();j++)
		{
			if(our_sub.count(test.edge[i][j].second))
				E_in++;
			if(our_obj.count(test.edge[i][j].first))
				E_out++;
		}
	double SF_Our=(double)E_in*E_out/(double)E_H*E*E;

	cout<<"SF_GJ: "<<SF_GJ<<endl;
	cout<<"SF_Our: "<<SF_Our<<endl;

	vector<double> cost=vector<double>(K+1);
	vector<int> last=vector<int>(K+1);
	for(int i=1;i<=K;i++)last[i]=i-1;
	cost[1]=E;
	for(int i=2;i<=K;i++)
	{
		cost[i]=SF_GJ*cost[i-1]*cost[1];
		for(int j=1;j<i-1;j++)
			if(cost[i]>SF_Our*cost[j]*cost[i-j-1])
			{
				cost[i]=SF_Our*cost[j]*cost[i-j-1];
				last[i]=j;
			}
	}

	for(int i=1;i<=K;i++)cout<<i<<": "<<last[i]<<endl;
}

int
main(int argc, char * argv[])
{
	//chdir(dirname(argv[0]));
//#ifdef DEBUG
	Util util;
//#endif

	if (argc == 1 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)
	{
		help();
		return 0;
	}
	cout << "gquery..." << endl;
	if (argc < 2)
	{
		cout << "error: lack of DB_store to be queried" << endl;
		return 0;
	}
	{
		cout << "argc: " << argc << "\t";
		cout << "DB_store:" << argv[1] << "\t";
		cout << endl;
	}

	string db_folder = string(argv[1]);
	int len = db_folder.length();
	if(db_folder.substr(len-3, 3) == ".db")
	{
		cout<<"your database can not end with .db"<<endl;
		return -1;
	}

	//if(db_folder[0] != '/' && db_folder[0] != '~')  //using relative path
	//{
	//db_folder = string("../") + db_folder;
	//}

	Database _db(db_folder);
	_db.load();
	cout << "finish loading" << endl;

	// read query from file.
	if (argc >= 3)
	{
		//        ifstream fin(argv[2]);
		//        if(!fin)
		//        {
		//            cout << "can not open: " << buf << endl;
		//            return 0;
		//        }
		//
		//        memset(buf, 0, sizeof(buf));
		//        stringstream _ss;
		//        while(!fin.eof()){
		//            fin.getline(buf, 9999);
		//            _ss << buf << "\n";
		//        }
		//        fin.close();
		//
		//        string query = _ss.str();

		string query = string(argv[2]);
		string store=string(argv[3]);
		string hash=string(argv[4]);
		//if(query[0] != '/' && query[0] != '~')  //using relative path
		//{
		//query = string("../") + query;
		//}
		struct timeval ST,ED;
    	gettimeofday(&ST,NULL);
		query = Util::getQueryFromFile(query.c_str());


		GeneralEvaluation general_evaluation(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
		vector<query_block> result;
		general_evaluation.decompose_query(query,result);
		for(int i=1;i<result.size();i++)
		{
			cout<<"query_block "<<i<<endl;
			if(result[i].pro_path)cout<<"Property Path!"<<endl;
			for(int j=0;j<result[i].edge.size();j++)
				cout<<result[i].edge[j]<<endl;
			if(result[i].st!=-1)cout<<"st: "<<result[i].st<<"\t"<<"ed: "<<result[i].ed<<endl;
			cout<<"connection:"<<endl;
			for(int j=0;j<result[i].connection.size();j++)
				cout<<result[i].connection[j].first<<" "<<result[i].connection[j].second<<endl;
			cout<<endl;
		}

		set<string> knownPoint;
		vector<map<string,int> > queryPoint(result.size(),map<string,int>());
		vector<vector<vector<string> > > block_result(result.size(),vector<vector<string> >());
		for(int i=1;i<result.size();i++)block_result[i].push_back(vector<string>());

	   	for(int i=1;i<result.size();i++)
		{
			cout<<endl<<"block: "<<i<<endl;
			int queryPoint_cnt=0;
			for(int j=0;j<result[i].edge.size();j++)
			{
				vector<string> tmp=Util::split(result[i].edge[j],"\t");
				if(tmp[0][0]=='?')
				{
					if(queryPoint[i].count(tmp[0])==0)
						queryPoint[i].insert(make_pair(tmp[0],1));
				}
				else knownPoint.insert(tmp[0]);
				if(tmp[1][0]=='?')
				{
					if(queryPoint[i].count(tmp[1])==0)
						queryPoint[i].insert(make_pair(tmp[1],1));
				}
				if(tmp[2][0]=='?')
				{
					if(queryPoint[i].count(tmp[2])==0)
						queryPoint[i].insert(make_pair(tmp[2],1));
				}
				else knownPoint.insert(tmp[2]);
			}
			string ssparql="";
			if(result[i].pro_path)
			{
				if(queryPoint[i].size()==2)
				{
					string start="select ";
					for(map<string,int>::iterator it=queryPoint[i].begin();it!=queryPoint[i].end();it++) it->second=queryPoint_cnt++,start+=it->first+" ";
					vector<vector<vector<string> > >candidate(result[i].edge.size(),vector<vector<string> >());
					for(int j=0;j<result[i].edge.size();j++)
					{
						ssparql=result[i].edge[j]+" .\n";
						ssparql=start+"where {\n"+ssparql+"}";
						cout<<ssparql<<endl;
						if(result[i].st==-1)Query(ssparql,block_result[i],_db);
						else Query(ssparql,candidate[j],_db);	

					}	
					if(result[i].st!=-1) 
					{
						St=result[i].st;
						K=result[i].ed;
						test.init();
   						test.loadGraph(candidate);
   						if(hash.length()>=4)
   						{
   							if(hash.length()==5)
   							{
   								pthread_t pid[2];
							   	for(int th=0;th<2;th++)
							   	{
							    	int* cur=new int(th);
							      	if(pthread_create(&pid[th],NULL,Thread_hash,cur)==-1)     
							        printf("thread%d fails!\n",th);
							   	}
							   	for(int th=0;th<2;th++)
							      	pthread_join(pid[th], NULL);
   							}
   							else
   							{
   								for(int x=0;x<2;x++)
							   	{
							   		_hash(x);
							   	}
   							}

						    vector<string> str;
						   	for(int th=0;th<2;th++)
						   	{
						    	T[th].output_hash(St,test,th*(-1),T[th].getRoot(),str,block_result[i]);
						   	}
   						}
   						else if(hash.length()==2)
   						{
   							dp(block_result[i]);
   						}
   						else
   						{
   							pthread_t pid[1];
						   	for(int th=0;th<1;th++)
						   	{
						    	int* cur=new int(th);
						      	if(pthread_create(&pid[th],NULL,Thread,cur)==-1)     
						        printf("thread%d fails!\n",th);
						   	}
						   	for(int th=0;th<1;th++)
						      	pthread_join(pid[th], NULL);

						    vector<string> str;
						   	for(int th=0;th<1;th++)
						   	{
						    	T[th].output(St,test,0,T[th].getRoot(),str,block_result[i]);
						   	}
   						}
					}
				}
				// else
				// {
				// 	string start="select ?s ?o";
				// 	for(int j=0;j<result[i].edge.size();j++)
				// 	{
				// 		ssparql=Util::split(result[i].edge[j],"\t")[1];
				// 		ssparql=start+"where {\n?s\t"+ssparql+"\t?o .\n}";
				// 		cout<<ssparql<<endl;
				// 		Query(ssparql,block_result[i]);
				// 	}
				// }
			}
			else
			{
				if(queryPoint[i].size())
				{
					string start="select ";
					for(map<string,int>::iterator it=queryPoint[i].begin();it!=queryPoint[i].end();it++) it->second=queryPoint_cnt++,start+=it->first+" ";
					for(int j=0;j<result[i].edge.size();j++)ssparql+=result[i].edge[j]+" .\n";
					ssparql=start+"where {\n"+ssparql+"}";
					cout<<ssparql<<endl;
					Query(ssparql,block_result[i],_db);
				}
				else
				{
					cout<<"all known"<<endl;
					block_result[i]=vector<vector<string> >(2,vector<string>());
				}
			}
			
		}

		if(block_result.size()==2)
		{
			ofstream out(store);
			for(int i=1;i<block_result[1].size();i++)
			{
				for(int j=0;j<block_result[1][i].size();j++)
					out<<block_result[1][i][j]<<"\t";
				out<<endl;
			}
			out.close();
			printf("Property path ans: %lld\n",block_result[1].size()-1);
			gettimeofday(&ED,NULL);
	   		float endtime=1000000*(ED.tv_sec-ST.tv_sec)+ED.tv_usec-ST.tv_usec;       
	   		endtime/=1000000;
	   		cout<<"join: "<<endtime<<" s"<<endl<<endl;
			return 0;
		}
		unordered_map<unsigned long long,vector<int> >* connectionToBlock=new unordered_map<unsigned long long,vector<int> >[result.size()];
		for(int i=1;i<result.size();i++)printf("block %d has candidate %lld\n",i,block_result[i].size());
		for(int i=1;i<result.size();i++)
        {
        	if(block_result[i].size()==1)
        		return 0;
        	for(int j=1;j<block_result[i].size();j++)
        	{
    			for(int k=0;k<result[i].connection.size();k++)
				{
					string connection_val=result[i].connection[k].first;
					if(connection_val[0]=='?')
					{
						int pos=queryPoint[i][connection_val];
						if(result[i].st!=-1&&pos==1)
							connection_val=block_result[i][j][block_result[i][j].size()-1];
						else	
							connection_val=block_result[i][j][pos];
					}
					unsigned int connection_block=result[i].connection[k].second;
					unsigned long long hash_val=Util::BKDRHash(connection_val.c_str());
					// hash_val=((hash_val<<4)&0x7FFFFFFF)|connection_block;
					hash_val=(hash_val<<4)|connection_block;
					if(connectionToBlock[i].count(hash_val)==0)
						connectionToBlock[i].emplace(hash_val,vector<int>());
					connectionToBlock[i][hash_val].push_back(j);
				}
        	}
        }

        int mn=block_result[1].size(),st=1;//find the start
		for(int i=1;i<result.size();i++)
		{
			if(mn>block_result[i].size())
			{
				mn=block_result[i].size();
				st=i;
			}
			cout<<i<<" "<<block_result[i].size()<<endl;
		}

		queue<pair<vector<int>,queue<int> > >q;
		int query_ans_cnt=0;
		for(int i=1;i<block_result[st].size();i++)
		{
			queue<int> tmp_queue;
			tmp_queue.push(st);
			vector<int> tmp_ans(result.size(),0);
			tmp_ans[st]=i;
			q.push(make_pair(tmp_ans,tmp_queue));
			while(!q.empty())
			{
				tmp_ans=q.front().first;
				tmp_queue=q.front().second;
				q.pop();
				if(tmp_queue.empty())	//get the result
				{
					// cout<<"final"<<endl;
					query_ans_cnt++;
					continue;
				}
				int t=tmp_queue.front();
				// cout<<t<<endl;
				tmp_queue.pop();
				int flag=1;
				queue<vector<int> >candidate_res;
				candidate_res.push(vector<int>());
				vector<int> candidateBlock;
				for(int j=0;j<result[t].connection.size();j++)
				{
					int b=result[t].connection[j].second;
					string cp=result[t].connection[j].first;

					if(tmp_ans[b])
					{
						if(cp[0]=='?')
						{
							int p1=queryPoint[t][cp],p2=queryPoint[b][cp];
							if(result[t].st!=-1&&p1==1)p1=block_result[t][tmp_ans[t]].size()-1;
							if(result[t].st!=-1&&p2==1)p2=block_result[b][tmp_ans[b]].size()-1;
							if(block_result[t][tmp_ans[t]][p1]!=block_result[b][tmp_ans[b]][p2])
							{
								flag=0;
								break;
							}
						}
					}
					else
					{
						bool go=1;
						for(int k=0;k<candidateBlock.size();k++)if(candidateBlock[k]==b){go=0;break;}
						if(!go)continue;

						if(cp[0]=='?')
						{
							int p=queryPoint[t][cp];
							if(result[t].st!=-1&&p==1)p=block_result[t][tmp_ans[t]].size()-1;
							cp=block_result[t][tmp_ans[t]][p];
						}
						unsigned long long hash_val=Util::BKDRHash(cp.c_str());
						hash_val=(hash_val<<4)|(unsigned int)t;
						if(connectionToBlock[b].count(hash_val)==0){flag=0;break;}
						candidateBlock.push_back(b);
						vector<int> candidate(connectionToBlock[b][hash_val]);
						while(true)
						{
							vector<int> last_candidate(candidate_res.front());
							if(last_candidate.size()<candidateBlock.size())candidate_res.pop();
							else break;
							for(int k=0;k<candidate.size();k++)
							{
								vector<int>next_candidate(last_candidate);
								next_candidate.push_back(candidate[k]);
								candidate_res.push(next_candidate);
							}
						}
						tmp_queue.push(b);
					} 
				}
				if(!flag)continue;
				while(!candidate_res.empty())
				{
					vector<int> tmp_candidate(candidate_res.front());
					candidate_res.pop();
					for(int j=0;j<candidateBlock.size();j++)
						tmp_ans[candidateBlock[j]]=tmp_candidate[j];
					q.push(make_pair(tmp_ans,tmp_queue));
				}
			}
		}
		cout<<"query_ans_cnt: "<<query_ans_cnt<<endl;
		delete[] connectionToBlock;
        return 0;
			
		if (query.empty())
		{
			return 0;
		}
		printf("query is:\n%s\n\n", query.c_str());
		ResultSet _rs;
		FILE* ofp = stdout;
		if (argc >= 4)
		{
			ofp = fopen(argv[3], "w");
		}
		string msg;
		int ret = _db.query(query, _rs, ofp);
		if(argc >= 4)
		{
			fclose(ofp);
			ofp = NULL;
		}

		//cout<<"gquery ret: "<<ret<<endl;
		if (ret <= -100)  //select query
		{
			if(ret == -100)
			{
				msg = _rs.to_str();
			}
			else //query error
			{
				msg = "query failed.";
			}
		}
		else //update query
		{
			if(ret >= 0)
			{
				msg = "update num: " + Util::int2string(ret);
			}
			else //update error
			{
				msg = "update failed.";
			}
		}
		if(ret != -100)
		{
			cout << msg <<endl;
		}

		return 0;
	}

	// read query file path from terminal.
	// BETTER: sighandler ctrl+C/D/Z
	string query;
	//char resolved_path[PATH_MAX+1];
#ifdef READLINE_ON
	char *buf, prompt[] = "gsql>";
	//const int commands_num = 3;
	//char commands[][20] = {"help", "quit", "sparql"};
	printf("Type `help` for information of all commands\n");
	printf("Type `help command_t` for detail of command_t\n");
	rl_bind_key('\t', rl_complete);
	while (true)
	{
		buf = readline(prompt);
		if (buf == NULL)
			continue;
		else
			add_history(buf);
		if (strncmp(buf, "help", 4) == 0)
		{
			if (strcmp(buf, "help") == 0)
			{
				//print commands message
				printf("help - print commands message\n");
				printf("quit - quit the console normally\n");
				printf("sparql - load query from the second argument\n");
			}
			else
			{
				//TODO: help for a given command
			}
			continue;
		}
		else if (strcmp(buf, "quit") == 0)
			break;
		else if (strncmp(buf, "sparql", 6) != 0)
		{
			printf("unknown commands\n");
			continue;
		}
		//TODO: sparql + string, not only path
		string query_file;
		//BETTER:build a parser for this console
		bool ifredirect = false;

		char* rp = buf;
		int pos = strlen(buf) - 1;
		while (pos > -1)
		{
			if (*(rp + pos) == '>')
			{
				ifredirect = true;
				break;
			}
			pos--;
		}
		rp += pos;

		char* p = buf + strlen(buf) - 1;
		FILE* fp = stdout;      ///default to output on screen
		if (ifredirect)
		{
			char* tp = p;
			while (*tp == ' ' || *tp == '\t')
				tp--;
			*(tp + 1) = '\0';
			tp = rp + 1;
			while (*tp == ' ' || *tp == '\t')
				tp++;
			fp = fopen(tp, "w");	//NOTICE:not judge here!
			p = rp - 1;					//NOTICE: all separated with ' ' or '\t'
		}
		while (*p == ' ' || *p == '\t')	//set the end of path
			p--;
		*(p + 1) = '\0';
		p = buf + 6;
		while (*p == ' ' || *p == '\t')	//acquire the start of path
			p++;
		//TODO: support the soft links(or hard links)
		//there are also readlink and getcwd functions for help
		//http://linux.die.net/man/2/readlink
		//NOTICE:getcwd and realpath cannot acquire the real path of file
		//in the same directory and the program is executing when the
		//system starts running
		//NOTICE: use realpath(p, NULL) is ok, but need to free the memory
		char* q = realpath(p, NULL);	//QUERY:still not work for soft links
#ifdef DEBUG_PRECISE
		printf("%s\n", p);
#endif
		if (q == NULL)
		{
			printf("invalid path!\n");
			free(q);
			free(buf);
			continue;
		}
		else
			printf("%s\n", q);
		//query = getQueryFromFile(p);
		query = Util::getQueryFromFile(q);
		if (query.empty())
		{
			free(q);
			//free(resolved_path);
			free(buf);
			if (ifredirect)
				fclose(fp);
			continue;
		}
		printf("query is:\n");
		printf("%s\n\n", query.c_str());
		ResultSet _rs;
		struct timeval ST,ED;
    	gettimeofday(&ST,NULL);
		int ret = _db.query(query, _rs, fp);
		string rs=_rs.to_str();
		string store="t.log";
		ofstream out(store.data());
		out<<rs;
		out.close();
		gettimeofday(&ED,NULL);
   		float endtime=1000000*(ED.tv_sec-ST.tv_sec)+ED.tv_usec-ST.tv_usec;       
   		endtime/=1000000;
   		cout<<"query time: "<<endtime<<" s"<<endl<<endl;
		//int ret = _db.query(query, _rs, NULL);
		string msg;

		//cout<<"gquery ret: "<<ret<<endl;
		if (ret <= -100)  //select query
		{
			if(ret == -100)
			{
				msg = "";
			}
			else //query error
			{
				msg = "query failed.";
			}
		}
		else //update query
		{
			if(ret >= 0)
			{
				msg = "update num: " + Util::int2string(ret);
			}
			else //update error
			{
				msg = "update failed.";
			}
		}

		if(ret != -100)
		{
			cout << msg << endl;
		}

		//test...
		//string answer_file = query_file+".out";
		//Util::save_to_file(answer_file.c_str(), _rs.to_str());
		free(q);
		//free(resolved_path);
		free(buf);
		if (ifredirect)
			fclose(fp);
#ifdef DEBUG_PRECISE
		printf("after buf freed!\n");
#endif
	}
	//#else					//DEBUG:this not work!
	//    while(true)
	//    {
	//        cout << "please input query file path:" << endl;
	//        string query_file;
	//        cin >> query_file;
	//        //char* q = realpath(query_file.c_str(), NULL);
	//        string query = getQueryFromFile(query_file.c_str());
	//        if(query.empty())
	//        {
	//            //free(resolved_path);
	//            continue;
	//        }
	//        cout << "query is:" << endl;
	//        cout << query << endl << endl;
	//        ResultSet _rs;
	//        _db.query(query, _rs, stdout);
	//        //free(resolved_path);
	//    }
#endif // READLINE_ON

	return 0;
}
