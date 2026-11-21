// Normalize the contest's restricted CSV dialect without repairing malformed rows.
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <cstdlib>
int main(int argc,char**argv) {
    if(argc<4) {std::cerr<<"usage: parse_csv WIDTH OUTPUT INPUT...\n";return 2;}
    const size_t width=std::stoul(argv[1]);
    std::ofstream out(argv[2],std::ios::binary);
    if(!out){std::cerr<<"cannot open output\n";return 1;}
    std::string line, normalized;
    std::vector<std::string_view> fields;fields.reserve(width);
    for(int a=3;a<argc;++a){
        std::ifstream in(argv[a],std::ios::binary);
        if(!in){std::cerr<<"cannot open input: "<<argv[a]<<'\n';return 1;}
        std::getline(in,line); // Exactly one header per input file.
        while(std::getline(in,line)){
            fields.clear();size_t start=0;bool valid=true;
            while(true){
                size_t comma=line.find(',',start);
                std::string_view f(line.data()+start,(comma==std::string::npos?line.size():comma)-start);
                if(f.size()>=2 && f.front()=='"' && f.back()=='"') {f.remove_prefix(1);f.remove_suffix(1);}
                if(f.find('"')!=std::string_view::npos){valid=false;break;}
                fields.push_back(f);
                if(fields.size()>width){valid=false;break;}
                if(comma==std::string::npos)break;
                start=comma+1;
            }
            if(!valid||fields.size()!=width)continue;
            normalized.clear();
            for(size_t i=0;i<width;++i){
                if(i)normalized+=',';
                normalized+='"';normalized.append(fields[i]);normalized+='"';
            }
            normalized+='\n';out.write(normalized.data(),normalized.size());
        }
        if(in.bad()){std::cerr<<"input read failed\n";return 1;}
    }
    out.close();if(!out){std::cerr<<"output write failed\n";return 1;}
}
