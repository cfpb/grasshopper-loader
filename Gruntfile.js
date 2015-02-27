'use strict';

module.exports = function (grunt) {

	//npm install
	grunt.registerTask('npm_install', 'install dependencies', function(){
	  var exec = require('child_process').exec;
		var cb = this.async();
		exec('npm install', {cwd: './'}, function(err, stdout){
		  console.log(stdout);
			cb();
		});
	});

  grunt.initConfig({
    //clean node_modules
	  clean: {
		  options: {
			  //force: true
			},
			node_modules: ['node-modules/*', '!node/modules/grunt**']
		},
		
		env: {
		  options: {  
			},
			test: {
			  NODE_ENV: 'test'
			}
		},

	  pkg: grunt.file.readJSON('package.json'),

	  uglify: {
		  options: {
			  banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
			},
	    build: {
			  src: ['/lib/**/*.js'],
	      dest: 'build/',
				ext: '.min.js'
			}
		},

		jshint: {
		  files: [
			  'shp2es.js',
			  'lib/**/*.js'
			],
			options: {
			  jshintrc: '.jshintrc'
			}
		},

	});

	grunt.loadNpmTasks('grunt-contrib-clean');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-develop');
  grunt.loadNpmTasks('grunt-env');
	grunt.loadNpmTasks('grunt-contrib-jshint');

	grunt.registerTask('default', ['jshint', 'uglify']);
  grunt.registerTask('clean', ['clean:node_modules', 'npm_install']);
  


};
