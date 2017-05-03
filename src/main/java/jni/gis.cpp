#include <iostream>
#include <jni.h>
#include "jni_JNIWrapper.h"
#include "include/resque.hpp"

using namespace std;

/* RESQUE JNI INTERFACE */
JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_resqueSPJ
(JNIEnv *env, jclass c, jobjectArray data, jint predicate, jint geomid1, jint geomid2)
{
  int size = env->GetArrayLength(data);
  /* initialize resque to handle spatial join query */
  Resque resq(predicate, geomid1, geomid2);
  /* populate datasets for join */
  for (int i=0; i<size; ++i){
    /* creating a new jstring here */
    jstring j_str = (jstring) env->GetObjectArrayElement(data, i);
    /* make a copy of the string for further c/c++ processing */
    string c_str = env->GetStringUTFChars(j_str, NULL);
    resq.populate(c_str);
    /* 
     * free memory to assist garbage collection by jvm 
     * since new jstring created in loop, it should also be released in the loop 
     */
    env->DeleteLocalRef(j_str);
  }

  /* data results for this tile */
  vector<string> hits = resq.join_bucket_spjoin();
  size = hits.size();

  /* return as String[] back to Java */
  jclass clazz = env->FindClass("java/lang/String");
  jobjectArray objarray = env->NewObjectArray(size ,clazz ,0);
  
  for(int i = 0; i < size; i++) {
    string s = hits[i]; 
    jstring js = (env)->NewStringUTF(s.c_str());
    (env)->SetObjectArrayElement(objarray , i , js);
  }
  return objarray;
}

JNIEXPORT jdouble JNICALL Java_jni_JNIWrapper_resqueTileDice
(JNIEnv *env, jobject c, jobjectArray data, jint predicate, jint geomid1, jint geomid2)
{
  int size = env->GetArrayLength(data);
  /* initialize resque */
  Resque resq(predicate, geomid1, geomid2);
  /* populate datasets for join */
  for (int i=0; i<size; ++i){
    
    jstring j_str = (jstring) env->GetObjectArrayElement(data, i);
    string c_str = env->GetStringUTFChars(j_str, NULL);
    resq.populate(c_str);
    /* free memory to assist garbage collection by jvm */
    env->DeleteLocalRef(j_str);
  }
  
  double tile_dice_result = resq.tile_dice();
  return tile_dice_result;

  //string tile_dice_result = resq.tile_dice();
  //jstring js = (env)->NewStringUTF(tile_dice_result.c_str());
  //return js;
}

JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_resqueKNN
(JNIEnv *env, jclass c, jobjectArray data, jint predicate, jint k, jint geomid1, jint geomid2)
{
  int size = env->GetArrayLength(data);
  /* initialize resque for kNN query */
  Resque resq(predicate, k, geomid1, geomid2);
  /* populate datasets for join */
  for (int i=0; i<size; ++i){
    
    jstring j_str = (jstring) env->GetObjectArrayElement(data, i);
    string c_str = env->GetStringUTFChars(j_str, NULL);
    resq.populate(c_str);
    /* free memory to assist garbage collection by jvm */
    env->DeleteLocalRef(j_str);
  }

  /* FIX: perform spatial query */
  resq.join_bucket_knn();
  
  vector<string> hits;
  size = hits.size();
  
  /* return as String[] back to Java */
  jclass clazz = env->FindClass("java/lang/String");
  jobjectArray objarray = env->NewObjectArray(size ,clazz ,0);
  
  for(int i = 0; i < size; i++) {
    string s = hits[i]; 
    jstring js = (env)->NewStringUTF(s.c_str());
    (env)->SetObjectArrayElement(objarray , i , js);
  }
  return objarray;
}

namespace Util{
  void tokenize (const string& str,
  		 vector<string>& result,
  		 const string& delimiters, 
  		 const bool keepBlankFields,
  		 const string& quote
  		 )
  {
    // clear the vector
    if ( false == result.empty() )
      {
	result.clear();
      }

    // you must be kidding
    if (delimiters.empty())
      return ;

    string::size_type pos = 0; // the current position (char) in the string
    char ch = 0; // buffer for the current character
    //char delimiter = 0;	// the buffer for the delimiter char which
    // will be added to the tokens if the delimiter
    // is preserved
    char current_quote = 0; // the char of the current open quote
    bool quoted = false; // indicator if there is an open quote
    string token;  // string buffer for the token
    bool token_complete = false; // indicates if the current token is
    // read to be added to the result vector
    string::size_type len = str.length();  // length of the input-string

    // for every char in the input-string
    while ( len > pos )
      {
	// get the character of the string and reset the delimiter buffer
	ch = str.at(pos);
	//delimiter = 0;

	bool add_char = true;

	// check ...

	// ... if the delimiter is a quote
	if ( false == quote.empty())
	  {
	    // if quote chars are provided and the char isn't protected
	    if ( string::npos != quote.find_first_of(ch) )
	      {
		// if not quoted, set state to open quote and set
		// the quote character
		if ( false == quoted )
		  {
		    quoted = true;
		    current_quote = ch;

		    // don't add the quote-char to the token
		    add_char = false;
		  }
		else // if quote is open already
		  {
		    // check if it is the matching character to close it
		    if ( current_quote == ch )
		      {
			// close quote and reset the quote character
			quoted = false;
			current_quote = 0;

			// don't add the quote-char to the token
			add_char = false;
		      }
		  } // else
	      }
	  }

	if ( false == delimiters.empty() && false == quoted )
	  {
	    // if ch is delemiter 
	    if ( string::npos != delimiters.find_first_of(ch) )
	      {
		token_complete = true;
		// don't add the delimiter to the token
		add_char = false;
	      }
	  }

	// add the character to the token
	if ( true == add_char )
	  {
	    // add the current char
	    token.push_back( ch );
	  }

	// add the token if it is complete
	// if ( true == token_complete && false == token.empty() )
	if ( true == token_complete )
	  {
	    if (token.empty())
	      {
		if (keepBlankFields)
		  result.push_back("");
	      }
	    else 
	      // add the token string
	      result.push_back( token );

	    // clear the contents
	    token.clear();

	    // build the next token
	    token_complete = false;

	  }
	// repeat for the next character
	++pos;
      } // while
    
    // add the final token
    if ( false == token.empty() ) {
      result.push_back( token );
    }
    else if(keepBlankFields && string::npos != delimiters.find_first_of(ch) ){
      result.push_back("");
    }
  }
}

/* 
 * Alternate approach 
 * Initialize Resque class object and return back its pointer. Use class pointer to populate data
 * For each string in java, call a jni function to populate the data in Resque class variables
 * when key changes call join_bucket() similar to original Hadoop-GIS
 * Disadvantage: Too many JNI calls
 */
// JNIEXPORT jlong JNICALL Java_jni_JNIWrapper_initResque
//   (JNIEnv *env, jclass c, jstring predicate, jint geomid1, jint geomid2)
// {
//   string p_str = env->GetStringUTFChars(predicate, NULL);
//   Resque *r = new Resque(p_str, geomid1, geomid2);
//   //Resque(p_str, geomid1, geomid2);
//   return (long) r;
// }

// JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_resque2
// (JNIEnv *env, jclass c, jlong objPtr, jstring line)
// {
 
//   Resque *resq = (Resque *)objPtr;
  
//   string c_str = env->GetStringUTFChars(line, NULL);
//   // if not same tileID, call join bucket
//   if (!resq->populate2(c_str)){
//     //vector<string> hits = resq->join_bucket();
//     //if (hits.size() > 0)
//     //  cout << "Got something ..." << hits.size() << endl;
//     // reset object
//     resq->reset();
//     // corner case: populate extra data
//     resq->populate_extra();
//   }
//   else
//     vector<string> hits = vector<string>();  
// }

/******************************** CODE BACKUP ***********************************/

// JNIEXPORT void JNICALL Java_jni_JNIWrapper_nativeFoo (JNIEnv *env, jclass c)
// {
//   cout << "C++ BAIG WAS HERE" << endl;
// }
// /*
//  * MUST BE CALLED PRIOR TO CALLING any other function
//  * Create index from partfile. 
//  * Return index pointer as long
//  */
// JNIEXPORT jlong JNICALL Java_jni_JNIWrapper_buildIndex
//   (JNIEnv *env, jclass c, jobjectArray partfile, jint geom_id)
// {
//   // // check if index created. If yes, no need to create again
//   // if (pmj == NULL){
//   //   cout << "Building index" << endl;
//   //   pmj = new PartitionMapperJoin(geom_id);
//   //   int size = env->GetArrayLength(partfile);
//   //   // generate tiles from partfile
//   //   for (int i=0; i<size; ++i){
//   //     jstring j_str = (jstring) env->GetObjectArrayElement(partfile, i);
//   //     string c_str = env->GetStringUTFChars(j_str, NULL);
//   //     pmj->gen_tile(c_str);
//   //     // free memory to assist garbage collection by jvm
//   //     env->DeleteLocalRef(j_str);
//   //   }
//   //   pmj->build_index();
//   //}
//   return -1;
// }

// JNIEXPORT void JNICALL Java_jni_JNIWrapper_pmjGarbageCollection
//   (JNIEnv *env , jclass c, jlong idx_ptr)
// {
//   cout << "Not Implemented Yet ..." << endl;
//   //partitionMapperJoin_free_mem(idx_ptr);
// }
